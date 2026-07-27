using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class AttachmentStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Migration012_CreatesAttachmentsTable()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_att_mig");
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*) FROM {schema.SchemaMigrationsTableSql} WHERE version = 12;
             """,
            connection);
        Assert.Equal(1L, (long)(await cmd.ExecuteScalarAsync())!);

        await using var exists = new NpgsqlCommand(
            $"SELECT to_regclass(@q) IS NOT NULL;",
            connection);
        exists.Parameters.AddWithValue("q", $"{schema.Schema}.attachments");
        Assert.True((bool)(await exists.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task SaveAsync_BindsConfirmedAttachments_OwnedBySender()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_att_bind_ok");
        var attachments = CreateAttachmentStore(client, schema);
        var messages = CreateMessageStore(client, schema);

        await attachments.InsertConfirmedAsync(Confirmed("a1", uploader: 10, key: "k1"));
        await attachments.InsertConfirmedAsync(Confirmed("a2", uploader: 10, key: "k2"));

        var result = await messages.SaveAsync(
            Message("m1", sender: 10, receiver: 20, attachmentIds: ["a1", "a2"]),
            Event("m1", target: 20));

        Assert.Equal(RealtimeMessagePersistKind.Created, result.Kind);

        var listed = await attachments.ListByMessageIdsAsync(["m1"]);
        Assert.Equal(2, listed.Count);
        Assert.All(listed, a =>
        {
            Assert.Equal(AttachmentStatus.Bound, a.Status);
            Assert.Equal("m1", a.MessageId);
            Assert.Equal("dm:10:20", a.ConversationId);
            Assert.NotNull(a.BoundAtMs);
        });
    }

    [Fact]
    public async Task SaveAsync_WrongOwner_FailsPermanentAndRollsBack()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_att_bind_owner");
        var attachments = CreateAttachmentStore(client, schema);
        var messages = CreateMessageStore(client, schema);

        await attachments.InsertConfirmedAsync(Confirmed("stolen", uploader: 99, key: "k-stolen"));

        var result = await messages.SaveAsync(
            Message("m-bad", sender: 10, receiver: 20, attachmentIds: ["stolen"]),
            Event("m-bad", target: 20));

        Assert.Equal(RealtimeMessagePersistKind.AttachmentBindFailed, result.Kind);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var msgCount = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.MessagesTableSql} WHERE message_id = 'm-bad';",
            connection);
        Assert.Equal(0L, (long)(await msgCount.ExecuteScalarAsync())!);

        var still = await attachments.ListByMessageIdsAsync(["m-bad"]);
        Assert.Empty(still);

        await using var statusCmd = new NpgsqlCommand(
            $"SELECT status FROM {schema.AttachmentsTableSql} WHERE attachment_id = 'stolen';",
            connection);
        Assert.Equal((short)AttachmentStatus.Confirmed, (short)(await statusCmd.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task DeleteByUser_RemovesAttachmentRows_AndReturnsObjectKeys()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_att_delete");
        var attachments = CreateAttachmentStore(client, schema);

        await attachments.InsertConfirmedAsync(Confirmed("d1", uploader: 7, key: "obj/7/d1"));
        await attachments.InsertConfirmedAsync(Confirmed("d2", uploader: 7, key: "obj/7/d2"));
        await attachments.InsertConfirmedAsync(Confirmed("keep", uploader: 8, key: "obj/8/keep"));

        var keys = await attachments.DeleteByUserAsync(7);
        Assert.Equal(2, keys.Count);
        Assert.Contains("obj/7/d1", keys);
        Assert.Contains("obj/7/d2", keys);

        Assert.Empty(await attachments.ListForUserExportAsync(7, null, 10));
        var remaining = await attachments.ListForUserExportAsync(8, null, 10);
        Assert.Single(remaining);
        Assert.Equal("keep", remaining[0].AttachmentId);
    }

    [Fact]
    public async Task InsertConfirmed_DuplicateClientAttachmentId_IsIdempotent()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_att_client_id");
        var attachments = CreateAttachmentStore(client, schema);

        var first = await attachments.InsertConfirmedAsync(
            Confirmed("c1", uploader: 3, key: "k-c1", clientId: "client-att-1"));
        var second = await attachments.InsertConfirmedAsync(
            Confirmed("c1", uploader: 3, key: "k-c1", clientId: "client-att-1"));

        Assert.Equal(first.AttachmentId, second.AttachmentId);
        Assert.Equal(first.ObjectKey, second.ObjectKey);

        var page = await attachments.ListForUserExportAsync(3, null, 10);
        Assert.Single(page);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateDatabaseAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }

    private static NpgsqlRealtimeAttachmentStore CreateAttachmentStore(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema) =>
        new(client, schema, NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);

    private static NpgsqlRealtimeMessageStore CreateMessageStore(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema) =>
        new(client, schema, TestMutationPolicy.Instance, NullLogger<NpgsqlRealtimeMessageStore>.Instance);

    private static RealtimeAttachmentRecord Confirmed(
        string id,
        long uploader,
        string key,
        string? clientId = null) =>
        new()
        {
            AttachmentId = id,
            UploaderUserId = uploader,
            ObjectKey = key,
            PublicUrl = $"https://cdn.example/{key}",
            ContentType = "image/png",
            SizeBytes = 128,
            OriginalName = $"{id}.png",
            Status = AttachmentStatus.Confirmed,
            ClientAttachmentId = clientId,
            CreatedAtMs = 1_700_000_000_000,
            ConfirmedAtMs = 1_700_000_000_100
        };

    private static RealtimeMessageRecord Message(
        string messageId,
        long sender,
        long receiver,
        IReadOnlyList<string>? attachmentIds) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "session-1",
            ReceiverUserId = receiver,
            ConversationId = ConversationId.CreateDirect(sender, receiver),
            Content = "with-attachments",
            AttachmentIds = attachmentIds,
            ReceivedAtMs = 1_700_000_000_200
        };

    private static RealtimeEvent Event(string messageId, long target) => new()
    {
        EventId = $"evt-{messageId}",
        Type = RealtimeEventType.MessageReceived,
        TargetUserId = target,
        ActorUserId = target == 20 ? 10 : 1,
        MessageId = messageId,
        OccurredAtMs = 1_700_000_000_200,
        PayloadJson = "{}"
    };
}

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class MessageIdempotencyFingerprintTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Npgsql_SameKeySameFingerprint_IsDuplicate()
    {
        var (client, schema) = await CreateStoreAsync("realtime_p1_fp_npgsql_dup");
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        var first = await store.SaveAsync(CreateMessage("m1", receiver: 2, content: "hello"), CreateEvent("m1"));
        var second = await store.SaveAsync(CreateMessage("m2", receiver: 2, content: "hello"), CreateEvent("m2"));

        Assert.Equal(RealtimeMessagePersistKind.Created, first.Kind);
        Assert.Equal(RealtimeMessagePersistKind.Duplicate, second.Kind);
        Assert.Equal(first.MessageId, second.MessageId);
    }

    [Fact]
    public async Task Npgsql_SameKeyDifferentContent_IsConflict()
    {
        var (client, schema) = await CreateStoreAsync("realtime_p1_fp_npgsql_conflict");
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        var first = await store.SaveAsync(CreateMessage("m1", receiver: 2, content: "hello"), CreateEvent("m1"));
        var conflict = await store.SaveAsync(CreateMessage("m2", receiver: 2, content: "other"), CreateEvent("m2"));

        Assert.Equal(RealtimeMessagePersistKind.Created, first.Kind);
        Assert.Equal(RealtimeMessagePersistKind.ContentConflict, conflict.Kind);
        Assert.Equal(first.MessageId, conflict.MessageId);
    }

    [Fact]
    public async Task Npgsql_SameContentSameAttachments_IsDuplicate()
    {
        var (client, schema) = await CreateStoreAsync("realtime_p1_fp_att_dup");
        var attachments = new NpgsqlRealtimeAttachmentStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        await attachments.InsertConfirmedAsync(Confirmed("att-a", uploader: 1));
        await attachments.InsertConfirmedAsync(Confirmed("att-b", uploader: 1));

        var first = await store.SaveAsync(
            CreateMessage("m1", receiver: 2, content: "hello", attachmentIds: ["att-b", "att-a"]),
            CreateEvent("m1"));
        var second = await store.SaveAsync(
            CreateMessage("m2", receiver: 2, content: "hello", attachmentIds: ["att-a", "att-b"]),
            CreateEvent("m2"));

        Assert.Equal(RealtimeMessagePersistKind.Created, first.Kind);
        Assert.Equal(RealtimeMessagePersistKind.Duplicate, second.Kind);
        Assert.Equal(first.MessageId, second.MessageId);
    }

    [Fact]
    public async Task Npgsql_SameContentDifferentAttachments_IsConflict()
    {
        var (client, schema) = await CreateStoreAsync("realtime_p1_fp_att_conflict");
        var attachments = new NpgsqlRealtimeAttachmentStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        await attachments.InsertConfirmedAsync(Confirmed("att-1", uploader: 1));
        await attachments.InsertConfirmedAsync(Confirmed("att-2", uploader: 1));

        var first = await store.SaveAsync(
            CreateMessage("m1", receiver: 2, content: "hello", attachmentIds: ["att-1"]),
            CreateEvent("m1"));
        var conflict = await store.SaveAsync(
            CreateMessage("m2", receiver: 2, content: "hello", attachmentIds: ["att-2"]),
            CreateEvent("m2"));

        Assert.Equal(RealtimeMessagePersistKind.Created, first.Kind);
        Assert.Equal(RealtimeMessagePersistKind.ContentConflict, conflict.Kind);
        Assert.Equal(first.MessageId, conflict.MessageId);
    }

    [Fact]
    public async Task EfCore_SameKeyDifferentReceiver_IsConflict()
    {
        const string schemaName = "realtime_p1_fp_ef_conflict";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
                .MigrateAsync(connection);
        }

        RealtimeDbContext.ConfigureSchema(schemaName);
        var options = new DbContextOptionsBuilder<RealtimeDbContext>()
            .UseNpgsql(connectionString)
            .Options;
        await using var factory = new TestDbContextFactory(options);
        var store = new EfCoreRealtimeMessageStore(
            factory,
            schema,
            NullLogger<EfCoreRealtimeMessageStore>.Instance);

        var first = await store.SaveAsync(CreateMessage("m1", receiver: 2, content: "hello"), CreateEvent("m1"));
        var conflict = await store.SaveAsync(CreateMessage("m2", receiver: 99, content: "hello"), CreateEvent("m2"));

        Assert.Equal(RealtimeMessagePersistKind.Created, first.Kind);
        Assert.Equal(RealtimeMessagePersistKind.ContentConflict, conflict.Kind);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateStoreAsync(
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

    private static RealtimeAttachmentRecord Confirmed(string id, long uploader) =>
        new()
        {
            AttachmentId = id,
            UploaderUserId = uploader,
            ObjectKey = $"obj/{id}",
            ContentType = "image/png",
            SizeBytes = 10,
            OriginalName = $"{id}.png",
            Status = AttachmentStatus.Confirmed,
            CreatedAtMs = 1,
            ConfirmedAtMs = 1
        };

    private static RealtimeMessageRecord CreateMessage(
        string messageId,
        long receiver,
        string content,
        IReadOnlyList<string>? attachmentIds = null) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = "client-same",
            SenderUserId = 1,
            SenderSessionId = "session-1",
            ReceiverUserId = receiver,
            ConversationId = ConversationId.CreateDirect(1, receiver),
            Content = content,
            AttachmentIds = attachmentIds,
            ReceivedAtMs = 1
        };

    private static RealtimeEvent CreateEvent(string messageId) =>
        new()
        {
            EventId = $"evt-{messageId}",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 2,
            MessageId = messageId,
            OccurredAtMs = 1
        };

    private sealed class TestDbContextFactory(DbContextOptions<RealtimeDbContext> options)
        : IDbContextFactory<RealtimeDbContext>, IAsyncDisposable
    {
        public RealtimeDbContext CreateDbContext() => new(options);

        public Task<RealtimeDbContext> CreateDbContextAsync(
            CancellationToken cancellationToken = default) =>
            Task.FromResult(CreateDbContext());

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}

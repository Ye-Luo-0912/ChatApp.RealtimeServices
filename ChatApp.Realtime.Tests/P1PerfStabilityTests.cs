using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class P1PerfStabilityTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task QueryCatchUps_Batch_IgnoresNonMemberConversation()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_p1_batch_catchup");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);

        var memberConvo = ConversationId.CreateDirect(101, 102);
        var otherConvo = ConversationId.CreateDirect(201, 202);
        await messageStore.SaveAsync(
            CreateMessage("m-member", 102, 101, memberConvo, "secret-member", 100),
            CreateEvent("evt-member", 101, "m-member"));
        await messageStore.SaveAsync(
            CreateMessage("m-other", 202, 201, otherConvo, "secret-other", 100),
            CreateEvent("evt-other", 201, "m-other"));

        var map = await historyStore.QueryCatchUpsAsync(
            userId: 101,
            [
                new HistoryCatchUpQuery { ConversationId = memberConvo, Take = 10 },
                new HistoryCatchUpQuery { ConversationId = otherConvo, Take = 10 }
            ]);

        Assert.Single(map[memberConvo]);
        Assert.Equal("secret-member", map[memberConvo][0].Content);
        Assert.Empty(map[otherConvo]);
    }

    [Fact]
    public async Task DuplicateSave_DoesNotRecreatePublishedOrDeadOutbox()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_p1_dup_outbox");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var outboxStore = new NpgsqlRealtimeOutboxStore(client, schema);

        const string eventId = "evt-stable-dup";
        var conversationId = ConversationId.CreateDirect(301, 302);
        var message = new RealtimeMessageRecord
        {
            MessageId = "msg-dup",
            ClientMessageId = "client-stable",
            SenderUserId = 301,
            SenderSessionId = "session-1",
            ReceiverUserId = 302,
            ConversationId = conversationId,
            Content = "hi",
            ReceivedAtMs = 50
        };
        var evt = CreateEvent(eventId, 302, "msg-dup");

        Assert.Equal(RealtimeMessagePersistKind.Created, (await messageStore.SaveAsync(message, evt)).Kind);
        var claimed = await outboxStore.ClaimBatchAsync("w1", 20, TimeSpan.FromMinutes(1));
        var record = Assert.Single(claimed, r => r.EventId == eventId);
        await outboxStore.MarkPublishedAsync(record);

        Assert.Equal(1, await CountOutboxAsync(client, schema, eventId));

        var dup = new RealtimeMessageRecord
        {
            MessageId = "msg-dup-2",
            ClientMessageId = "client-stable",
            SenderUserId = 301,
            SenderSessionId = "session-1",
            ReceiverUserId = 302,
            ConversationId = conversationId,
            Content = "hi",
            ReceivedAtMs = 50
        };
        Assert.Equal(
            RealtimeMessagePersistKind.Duplicate,
            (await messageStore.SaveAsync(dup, evt)).Kind);

        Assert.Equal(1, await CountOutboxAsync(client, schema, eventId));
        var item = await outboxStore.TryGetAsync(eventId);
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Published, item!.Status);

        await MarkStatusAsync(client, schema, eventId, RealtimeOutboxStatus.Dead);
        Assert.Equal(
            RealtimeMessagePersistKind.Duplicate,
            (await messageStore.SaveAsync(dup, evt)).Kind);
        item = await outboxStore.TryGetAsync(eventId);
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Dead, item!.Status);
        Assert.Equal(1, await CountOutboxAsync(client, schema, eventId));
    }

    [Fact]
    public async Task DeleteByUser_RemovesDirectConversationForBothPeers()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_p1_delete_dm");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);

        var conversationId = ConversationId.CreateDirect(401, 402);
        await messageStore.SaveAsync(
            CreateMessage("m1", 401, 402, conversationId, "from-deleted", 100),
            CreateEvent("e1", 402, "m1"));
        await messageStore.SaveAsync(
            CreateMessage("m2", 402, 401, conversationId, "from-peer", 200),
            CreateEvent("e2", 401, "m2"));

        var before = await conversationStore.QueryListAsync(
            402,
            beforeIsPinned: null,
            beforePinnedAtMs: null,
            beforeLastMessageAtMs: null,
            beforeConversationId: null,
            take: 10);
        Assert.Single(before);
        // m1 from 401→402 increments unread; m2 from 402→401 does not for 402.
        Assert.Equal(1, before[0].UnreadCount);

        await messageStore.DeleteByUserAsync(401);

        var after = await conversationStore.QueryListAsync(
            402,
            beforeIsPinned: null,
            beforePinnedAtMs: null,
            beforeLastMessageAtMs: null,
            beforeConversationId: null,
            take: 10);
        Assert.Empty(after);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.ConversationsTableSql} WHERE conversation_id = @cid",
            connection);
        cmd.Parameters.AddWithValue("cid", conversationId);
        Assert.Equal(0L, (long)(await cmd.ExecuteScalarAsync())!);
    }

    [Fact]
    public async Task ReplayDeadBatch_UpdatesAllInOneStatement()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_p1_replay_batch");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertDeadAsync(client, schema, "dead-a");
        await InsertDeadAsync(client, schema, "dead-b");
        await InsertDeadAsync(client, schema, "dead-c");

        var replayed = await store.ReplayDeadBatchAsync(["dead-a", "dead-b", "missing", "dead-a"]);
        Assert.Equal(2, replayed.Count);
        Assert.Contains("dead-a", replayed);
        Assert.Contains("dead-b", replayed);

        var claimed = await store.ClaimBatchAsync("batch-worker", 10, TimeSpan.FromSeconds(30));
        Assert.Equal(2, claimed.Count);
        Assert.Contains(claimed, x => x.EventId == "dead-a");
        Assert.Contains(claimed, x => x.EventId == "dead-b");

        var stillDead = await store.TryGetAsync("dead-c");
        Assert.Equal(RealtimeOutboxStatus.Dead, stillDead!.Status);
    }

    [Fact]
    public async Task MigrateAsync_ConcurrentCalls_DoNotCorruptSchema()
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema("realtime_p1_migrate_lock");
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        async Task MigrateOnceAsync()
        {
            await using var connection = await client.GetDataSource().OpenConnectionAsync();
            await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
                .MigrateAsync(connection);
        }

        await Task.WhenAll(MigrateOnceAsync(), MigrateOnceAsync(), MigrateOnceAsync());

        await using var check = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.SchemaMigrationsTableSql}",
            check);
        var applied = (long)(await cmd.ExecuteScalarAsync())!;
        Assert.True(applied >= 12);
    }

    [Fact]
    public async Task GetStats_OnlyCountsPendingAndDead()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_p1_stats_partial");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "p1");
        await InsertDeadAsync(client, schema, "d1");
        await InsertPublishedAsync(client, schema, "pub1");

        var stats = await store.GetStatsAsync();
        Assert.Equal(1, stats.PendingCount);
        Assert.Equal(1, stats.DeadCount);
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

    private static RealtimeMessageRecord CreateMessage(
        string messageId,
        long sender,
        long receiver,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "session-1",
            ReceiverUserId = receiver,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };

    private static RealtimeEvent CreateEvent(string eventId, long targetUserId, string messageId) =>
        new()
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = targetUserId,
            MessageId = messageId,
            OccurredAtMs = 1
        };

    private static async Task<long> CountOutboxAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.OutboxTableSql} WHERE event_id = @id",
            connection);
        cmd.Parameters.AddWithValue("id", eventId);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task MarkStatusAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId,
        RealtimeOutboxStatus status)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"UPDATE {schema.OutboxTableSql} SET status = @status WHERE event_id = @id",
            connection);
        cmd.Parameters.AddWithValue("status", (short)status);
        cmd.Parameters.AddWithValue("id", eventId);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertPendingAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (@id, @payload, 1, 5, 0, 1, 1, 0);
             """,
            connection);
        cmd.Parameters.AddWithValue("id", eventId);
        cmd.Parameters.AddWithValue(
            "payload",
            """{"EventId":"x","Type":5,"TargetUserId":1,"OccurredAtMs":1}""");
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertDeadAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (@id, @payload, 1, 5, 2, 1, 1, 9);
             """,
            connection);
        cmd.Parameters.AddWithValue("id", eventId);
        cmd.Parameters.AddWithValue(
            "payload",
            $$"""{"EventId":"{{eventId}}","Type":5,"TargetUserId":1,"OccurredAtMs":1}""");
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertPublishedAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, published_at_ms, attempt_count
             ) VALUES (@id, @payload, 1, 5, 1, 1, 1, 100, 1);
             """,
            connection);
        cmd.Parameters.AddWithValue("id", eventId);
        cmd.Parameters.AddWithValue(
            "payload",
            """{"EventId":"x","Type":5,"TargetUserId":1,"OccurredAtMs":1}""");
        await cmd.ExecuteNonQueryAsync();
    }
}

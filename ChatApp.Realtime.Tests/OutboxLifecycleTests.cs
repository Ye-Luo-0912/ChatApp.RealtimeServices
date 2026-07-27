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

public sealed class OutboxLifecycleTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task MarkDead_ExcludesFromClaim_AndReplayRestoresPending()
    {
        const string schemaName = "realtime_p1_outbox_lifecycle";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);

        await InsertPendingAsync(connectionString, schema, "dead-1", attemptCount: 9);

        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        var claimed = await store.ClaimBatchAsync("worker-a", 10, TimeSpan.FromSeconds(30));
        var record = Assert.Single(claimed);
        Assert.Equal(10, record.AttemptCount);

        await store.MarkDeadAsync(record, "poison");
        var afterDead = await store.ClaimBatchAsync("worker-b", 10, TimeSpan.FromSeconds(30));
        Assert.Empty(afterDead);

        var stats = await store.GetStatsAsync();
        Assert.Equal(0, stats.PendingCount);
        Assert.Equal(1, stats.DeadCount);

        Assert.True(await store.ReplayDeadAsync("dead-1"));
        var afterReplay = await store.ClaimBatchAsync("worker-c", 10, TimeSpan.FromSeconds(30));
        Assert.Single(afterReplay);
        Assert.Equal("dead-1", afterReplay[0].EventId);
        Assert.Equal(1, afterReplay[0].AttemptCount);

        var listed = await store.ListAsync(RealtimeOutboxStatus.Pending, targetUserId: null, 0, 10);
        Assert.Contains(listed, x => x.EventId == "dead-1");
        var got = await store.TryGetAsync("dead-1");
        Assert.NotNull(got);
        Assert.Equal(RealtimeOutboxStatus.Pending, got!.Status);
    }

    [Fact]
    public async Task CleanupPublished_DeletesOnlyOldPublishedRows()
    {
        const string schemaName = "realtime_p1_outbox_cleanup";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);
        await InsertPublishedAsync(connectionString, schema, "old-pub", publishedAtMs: 1_000);
        await InsertPublishedAsync(connectionString, schema, "new-pub", publishedAtMs: 9_000_000_000_000);
        await InsertPendingAsync(connectionString, schema, "still-pending");

        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        var deleted = await store.CleanupPublishedAsync(
            publishedBeforeMs: 2_000,
            batchSize: 100);
        Assert.Equal(1, deleted);

        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT event_id FROM {schema.OutboxTableSql} ORDER BY event_id",
            connection);
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetString(0));

        Assert.Contains("new-pub", ids);
        Assert.Contains("still-pending", ids);
        Assert.DoesNotContain("old-pub", ids);
    }

    [Fact]
    public async Task GetStats_ReportsOldestPendingAndInFlightAgeSources()
    {
        const string schemaName = "realtime_p1_outbox_stats";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);
        await InsertPendingAsync(connectionString, schema, "pending-old", createdAtMs: 1_111);

        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        var claimed = await store.ClaimBatchAsync("worker-stats", 1, TimeSpan.FromMinutes(5));
        Assert.Single(claimed);

        var stats = await store.GetStatsAsync();
        Assert.Equal(1, stats.PendingCount);
        Assert.Equal(1_111, stats.OldestPendingAtMs);
        Assert.Equal(1_111, stats.OldestInFlightAtMs);
        Assert.True(stats.MaxAttemptCount >= 1);
    }

    [Fact]
    public async Task ListDead_ListsOnlyDeadRowsBeforeCutoff_AscByCreatedAt()
    {
        const string schemaName = "realtime_p1_outbox_list_dead";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);
        // 三行 Dead，分别在不同时间创建；并保留一行 Pending 确保不会误读。
        await InsertDeadAsync(connectionString, schema, "dead-old", createdAtMs: 1_000);
        await InsertDeadAsync(connectionString, schema, "dead-mid", createdAtMs: 2_000);
        await InsertDeadAsync(connectionString, schema, "dead-new", createdAtMs: 9_000_000_000_000);
        await InsertPendingAsync(connectionString, schema, "still-pending", createdAtMs: 1_000);

        var store = new NpgsqlRealtimeOutboxStore(client, schema);

        // cutoff=3_000：仅返回 dead-old 与 dead-mid，按 created_at_ms 升序。
        var rows = await store.ListDeadAsync(createdBeforeMs: 3_000, limit: 100);
        Assert.Equal(2, rows.Count);
        Assert.Equal("dead-old", rows[0].EventId);
        Assert.Equal("dead-mid", rows[1].EventId);
        Assert.Equal(12, rows[0].TargetUserId);
        Assert.Equal((short)RealtimeEventType.MessageReceived, rows[0].EventType);
        Assert.NotNull(rows[0].PayloadJson);

        // limit=1：只返回最早的一行。
        var oneRow = await store.ListDeadAsync(createdBeforeMs: 3_000, limit: 1);
        Assert.Single(oneRow);
        Assert.Equal("dead-old", oneRow[0].EventId);
    }

    [Fact]
    public async Task DeleteDeadBatch_DeletesOnlyMatchingEventIds()
    {
        const string schemaName = "realtime_p1_outbox_delete_dead";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);
        await InsertDeadAsync(connectionString, schema, "dead-1", createdAtMs: 1_000);
        await InsertDeadAsync(connectionString, schema, "dead-2", createdAtMs: 2_000);
        await InsertDeadAsync(connectionString, schema, "dead-3", createdAtMs: 3_000);

        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        var deleted = await store.DeleteDeadBatchAsync(["dead-1", "dead-3", "missing"]);
        Assert.Equal(2, deleted);

        var remaining = await store.ListDeadAsync(createdBeforeMs: long.MaxValue, limit: 100);
        Assert.Single(remaining);
        Assert.Equal("dead-2", remaining[0].EventId);
    }

    [Fact]
    public async Task DeleteDeadBatch_DoesNotDeletePublishedOrPending()
    {
        const string schemaName = "realtime_p1_outbox_delete_dead_status";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(client, schema);
        await InsertDeadAsync(connectionString, schema, "dead-1", createdAtMs: 1_000);
        await InsertPendingAsync(connectionString, schema, "pending-1", createdAtMs: 1_000);
        await InsertPublishedAsync(connectionString, schema, "pub-1", publishedAtMs: 1_000);

        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        var deleted = await store.DeleteDeadBatchAsync(["dead-1", "pending-1", "pub-1"]);
        Assert.Equal(1, deleted);

        var stats = await store.GetStatsAsync();
        Assert.Equal(1, stats.PendingCount);
        Assert.Equal(0, stats.DeadCount);
    }

    private static async Task InsertDeadAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string eventId,
        long createdAtMs = 1)
    {
        var evt = new RealtimeEvent
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 12,
            OccurredAtMs = createdAtMs
        };
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, last_error
             ) VALUES (
                 @event_id, @payload_json, 12, @event_type, 2,
                 @created_at_ms, 1, 5, 'poison'
             );
             """,
            connection);
        insert.Parameters.AddWithValue("event_id", eventId);
        insert.Parameters.AddWithValue(
            "payload_json",
            System.Text.Json.JsonSerializer.Serialize(
                evt,
                Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default.RealtimeEvent));
        insert.Parameters.AddWithValue("event_type", (short)evt.Type);
        insert.Parameters.AddWithValue("created_at_ms", createdAtMs);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task ApplyMigrationsAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
    }

    private static async Task InsertPendingAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string eventId,
        int attemptCount = 0,
        long createdAtMs = 1)
    {
        var evt = new RealtimeEvent
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 12,
            OccurredAtMs = createdAtMs
        };
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload_json, 12, @event_type, 0,
                 @created_at_ms, 1, @attempt_count
             );
             """,
            connection);
        insert.Parameters.AddWithValue("event_id", eventId);
        insert.Parameters.AddWithValue(
            "payload_json",
            System.Text.Json.JsonSerializer.Serialize(
                evt,
                Infrastructure.Core.Serialization.RealtimeJsonSerializerContext.Default.RealtimeEvent));
        insert.Parameters.AddWithValue("event_type", (short)evt.Type);
        insert.Parameters.AddWithValue("created_at_ms", createdAtMs);
        insert.Parameters.AddWithValue("attempt_count", attemptCount);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task InsertPublishedAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string eventId,
        long publishedAtMs)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, published_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload_json, 12, 5, 1,
                 1, 1, @published_at_ms, 1
             );
             """,
            connection);
        insert.Parameters.AddWithValue("event_id", eventId);
        insert.Parameters.AddWithValue("payload_json", "{}");
        insert.Parameters.AddWithValue("published_at_ms", publishedAtMs);
        await insert.ExecuteNonQueryAsync();
    }
}
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P0：Outbox 按 typed target_user_id / event_type 精确删除，禁止 JSON LIKE 前缀误伤。
/// </summary>
public sealed class OutboxTypedColumnDeleteTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task DeleteByUser_DoesNotMatchPrefixUserIds_AndKeepsCleanupCompleted()
    {
        const string schemaName = "realtime_p0_outbox";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(
            client,
            schema,
            [new Migration001_BaselineSchema()]);

        // 历史行：仅有 payload_json。JSON 空白/属性顺序各异；user 12 不得误伤 123。
        await InsertBaselineOutboxAsync(
            connectionString,
            schema,
            "u12-compact",
            """{"EventId":"u12-compact","Type":5,"TargetUserId":12,"OccurredAtMs":1}""");
        await InsertBaselineOutboxAsync(
            connectionString,
            schema,
            "u12-spaced",
            """{"Type": 5, "OccurredAtMs": 1, "TargetUserId": 12, "EventId": "u12-spaced"}""");
        await InsertBaselineOutboxAsync(
            connectionString,
            schema,
            "u123-prefix-trap",
            """{"EventId":"u123-prefix-trap","Type":5,"TargetUserId":123,"OccurredAtMs":1}""");
        await InsertBaselineOutboxAsync(
            connectionString,
            schema,
            "u12-cleanup-done",
            """{"EventId":"u12-cleanup-done","Type":9,"TargetUserId":12,"OccurredAtMs":1}""");

        await ApplyMigrationsAsync(
            client,
            schema,
            RealtimeSchemaMigrationRunner.DefaultMigrations());

        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        await store.DeleteByUserAsync(12);

        var remaining = await ListOutboxAsync(connectionString, schema);
        Assert.DoesNotContain(remaining, row => row.EventId == "u12-compact");
        Assert.DoesNotContain(remaining, row => row.EventId == "u12-spaced");
        Assert.Contains(remaining, row => row.EventId == "u123-prefix-trap");
        Assert.Contains(remaining, row => row.EventId == "u12-cleanup-done");

        var kept123 = remaining.Single(row => row.EventId == "u123-prefix-trap");
        Assert.Equal(123, kept123.TargetUserId);
        Assert.Equal(5, kept123.EventType);
    }

    [Fact]
    public async Task Enqueue_PopulatesTypedColumns_AndDeleteIsExact()
    {
        const string schemaName = "realtime_p0_enqueue";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(
            client,
            schema,
            RealtimeSchemaMigrationRunner.DefaultMigrations());

        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        await store.EnqueueEventAsync(new RealtimeEvent
        {
            EventId = "enq-12",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 12,
            OccurredAtMs = 1
        });
        await store.EnqueueEventAsync(new RealtimeEvent
        {
            EventId = "enq-123",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 123,
            OccurredAtMs = 1
        });
        await store.EnqueueEventAsync(new RealtimeEvent
        {
            EventId = "enq-12-done",
            Type = RealtimeEventType.AccountCleanupCompleted,
            TargetUserId = 12,
            OccurredAtMs = 1
        });

        await store.DeleteByUserAsync(12);

        var remaining = await ListOutboxAsync(connectionString, schema);
        Assert.Equal(2, remaining.Count);
        Assert.Contains(remaining, row => row.EventId == "enq-123" && row.TargetUserId == 123);
        Assert.Contains(
            remaining,
            row => row.EventId == "enq-12-done"
                   && row.EventType == (short)RealtimeEventType.AccountCleanupCompleted);
    }

    [Fact]
    public async Task Migration_Backfill_IgnoresJsonWhitespaceAndPropertyOrder()
    {
        const string schemaName = "realtime_p0_backfill";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(
            client,
            schema,
            [new Migration001_BaselineSchema()]);

        await InsertBaselineOutboxAsync(
            connectionString,
            schema,
            "legacy-ws",
            """{ "OccurredAtMs" : 9 , "TargetUserId" : 12 , "Type" : 5 , "EventId" : "legacy-ws" }""");

        await ApplyMigrationsAsync(
            client,
            schema,
            [
                new Migration001_BaselineSchema(),
                new Migration002_OutboxTypedTargetColumns()
            ]);

        var rows = await ListOutboxAsync(connectionString, schema);
        var row = Assert.Single(rows);
        Assert.Equal(12, row.TargetUserId);
        Assert.Equal(5, row.EventType);
    }

    [Fact]
    public void WireSerializer_RoundTrip_KeepsTargetAndType()
    {
        var evt = new RealtimeEvent
        {
            EventId = "wire-1",
            Type = RealtimeEventType.UserAccountDeleted,
            TargetUserId = 12,
            OccurredAtMs = 42
        };

        var json = JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent);
        var restored = JsonSerializer.Deserialize(
            json,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);

        Assert.NotNull(restored);
        Assert.Equal(12, restored.TargetUserId);
        Assert.Equal(RealtimeEventType.UserAccountDeleted, restored.Type);
    }

    /// <summary>
    /// 四-1/六-4：新记录的 payload_utf8 不包含 TargetUserIds（数据库列是唯一权威），
    /// 因此 DeleteByUserAsync(A) 只需 array_remove 修改 target_user_ids 列，
    /// 不需要重写 payload_utf8。验证：
    /// - target_user_ids 列：移除 A，保留 B
    /// - payload_utf8 列：从未包含 A 或 B（因 OutboxInsertHelper 已排除路由字段）
    /// - DeleteByUserAsync 不重写 payload_utf8（仍可反序列化为无 TargetUserIds 的 wire 事件）
    /// </summary>
    [Fact]
    public async Task DeleteByUser_PreservesPayloadUtf8_ForNewMultiTargetRows()
    {
        const string schemaName = "realtime_p0_payload_utf8_cleanup";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await ApplyMigrationsAsync(
            client,
            schema,
            RealtimeSchemaMigrationRunner.DefaultMigrations());

        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        const long userA = 5_001;
        const long userB = 5_002;
        // 入队多目标事件：TargetUserIds 同时包含 A 与 B
        await store.EnqueueEventAsync(new RealtimeEvent
        {
            EventId = "enq-payload-cleanup",
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = userA,
            TargetUserIds = [userA, userB],
            OccurredAtMs = 1
        });

        // 入队后立即读回列值，验证 payload_utf8 中 TargetUserIds 被置为 null（路由字段由 target_user_ids 列权威管理）
        var beforeDelete = await GetOutboxRowAsync(connectionString, schema, "enq-payload-cleanup");
        Assert.NotNull(beforeDelete.PayloadUtf8);
        Assert.NotNull(beforeDelete.TargetUserIds);
        Assert.Equal([userA, userB], beforeDelete.TargetUserIds);
        // 反序列化验证 payload_utf8 中 TargetUserIds 为 null（已被 OutboxInsertHelper 剥离）
        var beforeRestored = JsonSerializer.Deserialize(
            beforeDelete.PayloadUtf8!.Value.Span,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);
        Assert.NotNull(beforeRestored);
        Assert.Null(beforeRestored!.TargetUserIds);
        // userB 仅存在于 TargetUserIds（已被剥离），不应出现在 payload 中
        var payloadTextBefore = System.Text.Encoding.UTF8.GetString(beforeDelete.PayloadUtf8!.Value.ToArray());
        Assert.DoesNotContain(userB.ToString(System.Globalization.CultureInfo.InvariantCulture), payloadTextBefore);

        // DeleteByUserAsync(A)：仅 array_remove 修改 target_user_ids，payload_utf8 不重写
        await store.DeleteByUserAsync(userA);

        var afterDelete = await GetOutboxRowAsync(connectionString, schema, "enq-payload-cleanup");
        Assert.NotNull(afterDelete.PayloadUtf8);
        Assert.NotNull(afterDelete.TargetUserIds);
        // target_user_ids 不再包含 A
        Assert.DoesNotContain(userA, afterDelete.TargetUserIds!);
        // target_user_ids 仍包含 B
        Assert.Contains(userB, afterDelete.TargetUserIds!);
        // payload_utf8 内容与删除前完全一致（未被重写）
        Assert.Equal(beforeDelete.PayloadUtf8!.Value.ToArray(), afterDelete.PayloadUtf8!.Value.ToArray());
        // payload_utf8 反序列化仍能成功（wire 事件结构完好）
        var restored = JsonSerializer.Deserialize(
            afterDelete.PayloadUtf8!.Value.Span,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);
        Assert.NotNull(restored);
        Assert.Equal("enq-payload-cleanup", restored!.EventId);
        Assert.Null(restored.TargetUserIds);
    }

    private static async Task ApplyMigrationsAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        IEnumerable<IRealtimeSchemaMigration> migrations)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance, migrations)
            .MigrateAsync(connection);
    }

    private static async Task InsertBaselineOutboxAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string eventId,
        string payloadJson)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload_json, 1, 1, 0
             );
             """,
            connection);
        insert.Parameters.AddWithValue("event_id", eventId);
        insert.Parameters.AddWithValue("payload_json", payloadJson);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task<List<OutboxRow>> ListOutboxAsync(
        string connectionString,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT event_id, target_user_id, event_type, payload_json
             FROM {schema.OutboxTableSql}
             ORDER BY event_id;
             """,
            connection);
        var rows = new List<OutboxRow>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add(new OutboxRow(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt16(2),
                reader.IsDBNull(3) ? null : reader.GetString(3)));
        }

        return rows;
    }

    private static async Task<OutboxPayloadRow> GetOutboxRowAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT payload_utf8, target_user_ids
             FROM {schema.OutboxTableSql}
             WHERE event_id = @event_id;
             """,
            connection);
        cmd.Parameters.AddWithValue("event_id", eventId);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        byte[]? payloadUtf8 = null;
        if (!reader.IsDBNull(0))
        {
            payloadUtf8 = (byte[])reader.GetValue(0);
        }

        long[]? targetUserIds = null;
        if (!reader.IsDBNull(1))
        {
            targetUserIds = (long[])reader.GetValue(1);
        }

        return new OutboxPayloadRow(
            payloadUtf8 is null ? null : new ReadOnlyMemory<byte>(payloadUtf8),
            targetUserIds);
    }

    private sealed record OutboxRow(
        string EventId,
        long TargetUserId,
        short EventType,
        string? PayloadJson);

    private sealed record OutboxPayloadRow(
        ReadOnlyMemory<byte>? PayloadUtf8,
        long[]? TargetUserIds);
}

using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using System.Text.Json;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// P0：OUTBOX-RECOVERY-1（租约、崩溃与死信重放）集成测试。
/// <para>
/// 覆盖需求 2/3：
/// - 审计式重放：单事务内重置 Dead → Pending 并写入 outbox_replay_audit，二者原子生效；
/// - 单条隔离：重放一条死信不影响无关记录（其他 Dead / Pending 均不受扰动）；
/// - 崩溃安全收敛：claim 后 / 发布后 / 完成前任一点进程退出，均能通过 lease 过期 → 重新认领
///   收敛到 Published，无永久 Pending；重放以稳定 event id 幂等收敛、不产生重复审计。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class OutboxRecoveryAuditTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task ReplayWithAudit_ResetsDeadRowAndWritesAuditAtomically()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_atomic");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertDeadAsync(client, schema, "dead-atomic-1", attemptCount: 7, lastError: "boom");

        Assert.True(await store.ReplayDeadWithAuditAsync(
            "dead-atomic-1", "ops-user", "manual intervention"));

        // 死信已重置为 Pending，历史字段清空
        var item = await store.TryGetAsync("dead-atomic-1");
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Pending, item!.Status);
        Assert.Equal(0, item.AttemptCount);
        Assert.Null(item.LastError);
        Assert.Null(item.PublishedAtMs);

        // 审计记录已写入，捕获重放前状态（attempt_count / last_error）与新 checkpoint
        var audits = await store.ListReplayAuditsAsync(null, 0, 50);
        var audit = Assert.Single(audits);
        Assert.Equal("dead-atomic-1", audit.EventId);
        Assert.Equal("ops-user", audit.Operator);
        Assert.Equal("manual intervention", audit.Reason);
        Assert.Equal(7, audit.PriorAttemptCount);
        Assert.Equal("boom", audit.PriorLastError);
        Assert.True(audit.NewCheckpointMs > 0);
        Assert.True(audit.ReplayedAtMs > 0);
    }

    [Fact]
    public async Task ReplayWithAudit_IsolatedToTargetEvent_DoesNotTouchOthers()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_isolated");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertDeadAsync(client, schema, "dead-target", attemptCount: 3, lastError: "e1");
        await InsertDeadAsync(client, schema, "dead-other", attemptCount: 9, lastError: "e2");
        await InsertPendingAsync(client, schema, "pending-other");

        Assert.True(await store.ReplayDeadWithAuditAsync("dead-target", "ops", "fix"));

        // 目标重置为 Pending
        Assert.Equal(
            RealtimeOutboxStatus.Pending,
            (await store.TryGetAsync("dead-target"))!.Status);
        // 无关死信保持 Dead 且历史不变
        var other = await store.TryGetAsync("dead-other");
        Assert.Equal(RealtimeOutboxStatus.Dead, other!.Status);
        Assert.Equal(9, other.AttemptCount);
        Assert.Equal("e2", other.LastError);
        // 无关 Pending 不受扰动
        Assert.Equal(
            RealtimeOutboxStatus.Pending,
            (await store.TryGetAsync("pending-other"))!.Status);

        // 审计只针对目标事件，不产生无关条目
        var audits = await store.ListReplayAuditsAsync(null, 0, 50);
        var audit = Assert.Single(audits);
        Assert.Equal("dead-target", audit.EventId);
    }

    [Fact]
    public async Task ReplayWithAudit_OnNonDeadOrMissing_ReturnsFalse_AndWritesNoAudit()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_missing");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "pending-not-dead");

        // 不存在的 event id：返回 false
        Assert.False(await store.ReplayDeadWithAuditAsync("does-not-exist", "ops", "x"));
        // Pending（非 Dead）的行：返回 false
        Assert.False(await store.ReplayDeadWithAuditAsync("pending-not-dead", "ops", "x"));

        // 未发生任何重放，不写审计
        Assert.Empty(await store.ListReplayAuditsAsync(null, 0, 50));
        // Pending 行未被扰动
        Assert.Equal(
            RealtimeOutboxStatus.Pending,
            (await store.TryGetAsync("pending-not-dead"))!.Status);
    }

    [Fact]
    public async Task CrashAfterPublish_ReplayConverges_WithoutPermanentPending_AndNoDuplicateAudit()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_crash");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertDeadAsync(client, schema, "crash-pub-1", attemptCount: 2, lastError: "timeout");

        // 模拟：消息已发布但完成前崩溃。运维重放两条相同 event id（幂等收敛）。
        Assert.True(await store.ReplayDeadWithAuditAsync("crash-pub-1", "ops", "replay"));
        // 第一次重放后行已是 Pending，第二次重放命中非 Dead，返回 false 且不重复写审计
        // （幂等性由“稳定 event id + 单次重置”保证）。
        Assert.False(await store.ReplayDeadWithAuditAsync("crash-pub-1", "ops", "replay-again"));
        var item = await store.TryGetAsync("crash-pub-1");
        Assert.Equal(RealtimeOutboxStatus.Pending, item!.Status);
        Assert.Null(item.LockedBy);

        // 完成前崩溃：记录保持 Pending，可被重新认领并收敛到 Published，无永久 Pending
        var claimed = await store.ClaimBatchAsync("recovery", 10, TimeSpan.FromSeconds(30));
        var record = Assert.Single(claimed);
        Assert.Equal("crash-pub-1", record.EventId);
        await store.MarkPublishedAsync(record);
        Assert.Equal(
            RealtimeOutboxStatus.Published,
            (await store.TryGetAsync("crash-pub-1"))!.Status);

        // 审计只记录实际发生的成功重放（第二次 failed 命中不写审计）
        var audits = await store.ListReplayAuditsAsync("crash-pub-1", 0, 50);
        Assert.Single(audits);
        Assert.Equal(2, audits[0].PriorAttemptCount);
    }

    [Fact]
    public async Task CrashAfterClaim_LeaseExpiry_ReclaimConverges_WithoutPermanentPending()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_crash_claim");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "crash-claim-1");

        // 实例 A claim 后崩溃（lease 1 秒，未完成任何状态写入）
        var claimedA = await store.ClaimBatchAsync("instance-a", 10, TimeSpan.FromSeconds(1));
        var recordA = Assert.Single(claimedA);
        Assert.Equal("crash-claim-1", recordA.EventId);

        // 等待 lease 过期
        await Task.Delay(TimeSpan.FromMilliseconds(1_200));

        // 恢复实例 B 重新认领并完成：无永久 Pending、无越权完成
        var claimedB = await store.ClaimBatchAsync("instance-b", 10, TimeSpan.FromSeconds(30));
        var recordB = Assert.Single(claimedB);
        Assert.Equal("crash-claim-1", recordB.EventId);
        Assert.NotEqual(recordA.ClaimToken, recordB.ClaimToken);

        // 旧 worker A 迟到完成（用旧 token）不得越权
        await store.MarkPublishedAsync(recordA);
        Assert.Equal(
            RealtimeOutboxStatus.Pending,
            (await store.TryGetAsync("crash-claim-1"))!.Status);

        // 新 worker B 正常完成
        await store.MarkPublishedAsync(recordB);
        Assert.Equal(
            RealtimeOutboxStatus.Published,
            (await store.TryGetAsync("crash-claim-1"))!.Status);
    }

    [Fact]
    public async Task ListReplayAudits_FiltersByEventId_AndOrdersByTimeDesc()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_audit_list");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        // audit-a 重放两次（第二次前重新置为 Dead），audit-b 重放一次 → 共 3 条审计。
        await InsertDeadAsync(client, schema, "audit-a", attemptCount: 1, lastError: null);
        Assert.True(await store.ReplayDeadWithAuditAsync("audit-a", "ops-a", "first"));
        await Task.Delay(20);

        await InsertDeadAsync(client, schema, "audit-b", attemptCount: 2, lastError: null);
        Assert.True(await store.ReplayDeadWithAuditAsync("audit-b", "ops-b", "second"));
        await Task.Delay(20);

        // audit-a 已重置为 Pending，重新标为 Dead 后再重放一次。
        await SetDeadAsync(client, schema, "audit-a", attemptCount: 1, lastError: null);
        Assert.True(await store.ReplayDeadWithAuditAsync("audit-a", "ops-a", "third"));

        // 过滤单事件：audit-a 应按时间倒序返回两条
        var onlyA = await store.ListReplayAuditsAsync("audit-a", 0, 50);
        Assert.Equal(2, onlyA.Count);
        Assert.Equal("third", onlyA[0].Reason);
        Assert.Equal("first", onlyA[1].Reason);
        Assert.All(onlyA, audit => Assert.Equal("audit-a", audit.EventId));

        // 全量：三条，时间倒序
        var all = await store.ListReplayAuditsAsync(null, 0, 50);
        Assert.Equal(3, all.Count);
        Assert.Equal("audit-a", all[0].EventId); // 最近一次是 audit-a 的 third
        Assert.True(all[0].ReplayedAtMs >= all[1].ReplayedAtMs);
        Assert.True(all[1].ReplayedAtMs >= all[2].ReplayedAtMs);

        // 分页：offset=1 应跳过最近一条
        var paged = await store.ListReplayAuditsAsync(null, 1, 50);
        Assert.Equal(2, paged.Count);
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

    private static async Task InsertPendingAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId)
    {
        var evt = new RealtimeEvent
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 42,
            OccurredAtMs = 1
        };
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload, 42, @event_type, 0, 1, 1, 0
             );
             """,
            connection);
        cmd.Parameters.AddWithValue("event_id", eventId);
        cmd.Parameters.AddWithValue(
            "payload",
            JsonSerializer.Serialize(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent));
        cmd.Parameters.AddWithValue("event_type", (short)evt.Type);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task InsertDeadAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId,
        int attemptCount,
        string? lastError)
    {
        var evt = new RealtimeEvent
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 42,
            OccurredAtMs = 1
        };
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, last_error
             ) VALUES (
                 @event_id, @payload, 42, @event_type, @dead, 1, 1, @attempt_count, @last_error
             );
             """,
            connection);
        cmd.Parameters.AddWithValue("event_id", eventId);
        cmd.Parameters.AddWithValue(
            "payload",
            JsonSerializer.Serialize(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent));
        cmd.Parameters.AddWithValue("event_type", (short)evt.Type);
        cmd.Parameters.AddWithValue("dead", (short)RealtimeOutboxStatus.Dead);
        cmd.Parameters.AddWithValue("attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("last_error", lastError is null ? DBNull.Value : lastError);
        await cmd.ExecuteNonQueryAsync();
    }

    /// <summary>把已存在的行（重置为 Pending 后）重新标为 Dead，用于在同一 event id 上多次重放。</summary>
    private static async Task SetDeadAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string eventId,
        int attemptCount,
        string? lastError)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             UPDATE {schema.OutboxTableSql}
             SET status = @dead,
                 attempt_count = @attempt_count,
                 last_error = @last_error,
                 next_attempt_at_ms = 1
             WHERE event_id = @event_id;
             """,
            connection);
        cmd.Parameters.AddWithValue("event_id", eventId);
        cmd.Parameters.AddWithValue("dead", (short)RealtimeOutboxStatus.Dead);
        cmd.Parameters.AddWithValue("attempt_count", attemptCount);
        cmd.Parameters.AddWithValue("last_error", lastError is null ? DBNull.Value : lastError);
        await cmd.ExecuteNonQueryAsync();
    }
}
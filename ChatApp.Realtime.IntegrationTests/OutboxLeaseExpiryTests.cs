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
/// P1-3：Outbox lease 过期与 claim_token 所有权校验集成测试。
/// <para>
/// 验证 NpgsqlRealtimeOutboxStore 的 lease 语义：
/// - lease 过期后其他实例可重新 claim；
/// - claim_token 不匹配时 MarkPublished/MarkFailed 不生效（防止旧任务误完成新 lease）；
/// - ExtendLeaseBatch 仅续租 claim_token 匹配的记录；
/// - 同一 instanceId 在 lease 过期后重新 claim 会获得新 claim_token。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class OutboxLeaseExpiryTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task ClaimedRecord_CanBeReclaimedByOtherInstance_AfterLeaseExpires()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_lease_expiry");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "lease-expire-1");

        // 实例 A 以 1 秒 lease 认领
        var claimedA = await store.ClaimBatchAsync("instance-a", 10, TimeSpan.FromSeconds(1));
        var recordA = Assert.Single(claimedA);
        Assert.Equal("lease-expire-1", recordA.EventId);
        Assert.Equal("instance-a", recordA.LockOwner);
        var tokenA = recordA.ClaimToken;
        Assert.NotEmpty(tokenA);

        // lease 未过期时，实例 B 无法认领
        var claimedB = await store.ClaimBatchAsync("instance-b", 10, TimeSpan.FromSeconds(30));
        Assert.Empty(claimedB);

        // 等待 lease 过期（额外缓冲确保 locked_until_ms < now）
        await Task.Delay(TimeSpan.FromMilliseconds(1_200));

        // lease 过期后，实例 B 可重新认领
        var claimedB2 = await store.ClaimBatchAsync("instance-b", 10, TimeSpan.FromSeconds(30));
        var recordB = Assert.Single(claimedB2);
        Assert.Equal("lease-expire-1", recordB.EventId);
        Assert.Equal("instance-b", recordB.LockOwner);
        // P1-3：新 claim 必须生成新 claim_token，防止 A 用旧 token 完成 B 的 lease
        Assert.NotEqual(tokenA, recordB.ClaimToken);
    }

    [Fact]
    public async Task MarkPublished_WithMismatchedClaimToken_DoesNotUpdateRow()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_claim_token_mismatch");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "token-mismatch-1");

        var claimed = await store.ClaimBatchAsync("instance-x", 10, TimeSpan.FromSeconds(30));
        var record = Assert.Single(claimed);
        Assert.Equal("token-mismatch-1", record.EventId);

        // 构造一个 claim_token 不匹配的 record（模拟旧 lease 残留任务误完成新 lease）
        var forgedRecord = record with { ClaimToken = "forged-token-not-matching" };

        await store.MarkPublishedAsync(forgedRecord);

        // 状态不应变为 Published
        var item = await store.TryGetAsync("token-mismatch-1");
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Pending, item!.Status);

        // 用正确的 claim_token 可正常完成
        await store.MarkPublishedAsync(record);
        var after = await store.TryGetAsync("token-mismatch-1");
        Assert.NotNull(after);
        Assert.Equal(RealtimeOutboxStatus.Published, after!.Status);
    }

    [Fact]
    public async Task MarkFailed_WithMismatchedClaimToken_DoesNotUpdateRow()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_markfailed_mismatch");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "failed-mismatch-1");

        var claimed = await store.ClaimBatchAsync("instance-y", 10, TimeSpan.FromSeconds(30));
        var record = Assert.Single(claimed);

        var forgedRecord = record with { ClaimToken = "wrong-token" };

        await store.MarkFailedAsync(forgedRecord, "fake-error", TimeSpan.FromSeconds(5));

        // last_error 不应被写入
        var item = await store.TryGetAsync("failed-mismatch-1");
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Pending, item!.Status);
        Assert.Null(item.LastError);

        // 用正确 token 调用 MarkFailedAsync
        await store.MarkFailedAsync(record, "real-error", TimeSpan.FromSeconds(5));
        var after = await store.TryGetAsync("failed-mismatch-1");
        Assert.NotNull(after);
        Assert.Equal(RealtimeOutboxStatus.Pending, after!.Status);
        Assert.Equal("real-error", after.LastError);
    }

    [Fact]
    public async Task ExtendLeaseBatch_OnlyExtendsMatchingClaimTokens()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_extend_lease");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "extend-1");
        await InsertPendingAsync(client, schema, "extend-2");

        var claimed = await store.ClaimBatchAsync("instance-z", 10, TimeSpan.FromSeconds(30));
        Assert.Equal(2, claimed.Count);

        // 为 extend-1 构造错误 token，extend-2 保留正确 token
        var record1 = claimed.Single(r => r.EventId == "extend-1");
        var record2 = claimed.Single(r => r.EventId == "extend-2");
        var forgedRecord1 = record1 with { ClaimToken = "wrong-token" };

        var extended = await store.ExtendLeaseBatchAsync(
            [forgedRecord1, record2],
            TimeSpan.FromMinutes(5));

        // 只有 token 匹配的 extend-2 被续租
        Assert.Equal(1, extended);

        // 验证 extend-2 的 locked_until_ms 已被延长
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT event_id, locked_until_ms
             FROM {schema.OutboxTableSql}
             WHERE event_id = ANY(@ids)
             ORDER BY event_id
             """,
            connection);
        cmd.Parameters.AddWithValue("ids", new[] { "extend-1", "extend-2" });
        var rows = new List<(string EventId, long? LockedUntilMs)>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            rows.Add((reader.GetString(0), reader.IsDBNull(1) ? null : reader.GetInt64(1)));
        }

        var row1 = rows.Single(r => r.EventId == "extend-1");
        var row2 = rows.Single(r => r.EventId == "extend-2");
        // extend-1 的 locked_until_ms 应保持原值（30s lease 内）
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        Assert.True(row1.LockedUntilMs < nowMs + 60_000);
        // extend-2 的 locked_until_ms 应被延长到 5 分钟后
        Assert.True(row2.LockedUntilMs > nowMs + 60_000);
    }

    [Fact]
    public async Task MarkPublishedBatch_WithMismatchedTokens_ReturnsZero()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_batch_mismatch");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "batch-1");
        await InsertPendingAsync(client, schema, "batch-2");

        var claimed = await store.ClaimBatchAsync("instance-batch", 10, TimeSpan.FromSeconds(30));
        Assert.Equal(2, claimed.Count);

        // 全部使用错误 token
        var forged = claimed
            .Select(r => r with { ClaimToken = "all-wrong" })
            .ToArray();

        var affected = await store.MarkPublishedBatchAsync(forged);
        Assert.Equal(0, affected);

        // 用正确 token 应批量成功
        var affectedOk = await store.MarkPublishedBatchAsync(claimed);
        Assert.Equal(2, affectedOk);
    }

    [Fact]
    public async Task SameInstance_ReclaimAfterExpiry_GeneratesNewClaimToken()
    {
        var (client, schema) = await CreateStoreAsync("rt_outbox_reclaim_token");
        var store = new NpgsqlRealtimeOutboxStore(client, schema);
        await InsertPendingAsync(client, schema, "reclaim-1");

        // 同一 instanceId 第一次认领
        var claimed1 = await store.ClaimBatchAsync("instance-reclaim", 10, TimeSpan.FromSeconds(1));
        var record1 = Assert.Single(claimed1);
        var token1 = record1.ClaimToken;

        // 等待 lease 过期
        await Task.Delay(TimeSpan.FromMilliseconds(1_200));

        // 同一 instanceId 再次认领：应获得新 claim_token
        var claimed2 = await store.ClaimBatchAsync("instance-reclaim", 10, TimeSpan.FromSeconds(30));
        var record2 = Assert.Single(claimed2);
        Assert.NotEqual(token1, record2.ClaimToken);

        // 旧 token 不再有效
        await store.MarkPublishedAsync(record1);
        var item = await store.TryGetAsync("reclaim-1");
        Assert.NotNull(item);
        Assert.Equal(RealtimeOutboxStatus.Pending, item!.Status);

        // 新 token 有效
        await store.MarkPublishedAsync(record2);
        var after = await store.TryGetAsync("reclaim-1");
        Assert.NotNull(after);
        Assert.Equal(RealtimeOutboxStatus.Published, after!.Status);
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
}

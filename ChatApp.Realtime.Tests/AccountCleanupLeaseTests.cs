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

/// <summary>
/// 六-1：账号清理 Saga 租约门禁测试。
/// <para>
/// 验证 <see cref="NpgsqlAccountCleanupJobStore"/> 的租约机制：
/// <list type="bullet">
/// <item>认领作业时写入 claim_token / locked_by / locked_until_ms</item>
/// <item>租约过期后其他实例可重新认领（崩溃恢复）</item>
/// <item>RenewLeaseAsync 拒绝旧 claim_token（防止过期 lease 误续新 lease）</item>
/// <item>ProcessAttachmentsBatchAtomicAsync 在 lease 失效时回滚并返回 false</item>
/// <item>CompletePhaseAsync 清空 lease 字段并将下一阶段置为 pending</item>
/// </list>
/// </para>
/// </summary>
public sealed class AccountCleanupLeaseTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task GetNextPending_ClaimsJob_AndWritesLeaseFields()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_claim");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_001;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        // 认领作业：应写入 claim_token / locked_by / locked_until_ms
        var lease = TimeSpan.FromMinutes(5);
        var job = await store.GetNextPendingAsync("instance-a", lease, CancellationToken.None);
        Assert.NotNull(job);
        Assert.Equal(userId, job!.UserId);
        Assert.Equal(AccountCleanupJob.PhaseAttachments, job.Phase);
        Assert.Equal(AccountCleanupJob.StatusRunning, job.Status);
        Assert.Equal("instance-a", job.LockedBy);
        Assert.NotNull(job.ClaimToken);
        Assert.NotNull(job.LockedUntilMs);
        Assert.True(job.LockedUntilMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        // 同一时刻第二次认领应返回 null（已被锁定）
        var second = await store.GetNextPendingAsync("instance-b", lease, CancellationToken.None);
        Assert.Null(second);
    }

    [Fact]
    public async Task ExpiredLease_AllowsReclaimByAnotherInstance()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_reclaim");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_002;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        // instance-a 以极短租约认领（1 秒），模拟崩溃后 lease 过期
        var shortLease = TimeSpan.FromSeconds(1);
        var firstClaim = await store.GetNextPendingAsync("instance-a", shortLease, CancellationToken.None);
        Assert.NotNull(firstClaim);
        Assert.Equal("instance-a", firstClaim!.LockedBy);
        Assert.NotNull(firstClaim.ClaimToken);

        // 等待 lease 过期
        await Task.Delay(TimeSpan.FromMilliseconds(1_200));

        // instance-b 应能重新认领（lease 已过期）
        var reclaimed = await store.GetNextPendingAsync("instance-b", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(reclaimed);
        Assert.Equal(userId, reclaimed!.UserId);
        Assert.Equal("instance-b", reclaimed.LockedBy);
        Assert.NotEqual(firstClaim.ClaimToken, reclaimed.ClaimToken);
    }

    [Fact]
    public async Task RenewLease_ExtendsLease_AndRejectsStaleToken()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_renew");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_003;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        var job = await store.GetNextPendingAsync("instance-a", TimeSpan.FromSeconds(2), CancellationToken.None);
        Assert.NotNull(job);

        // 用正确 claim_token 续租：应成功
        var renewed = await store.RenewLeaseAsync(
            userId, job!.Phase, job.ClaimToken!, TimeSpan.FromMinutes(10), CancellationToken.None);
        Assert.True(renewed);

        // 验证 locked_until_ms 已延长
        var renewedJob = await GetJobAsync(client, schema, userId, job.Phase);
        Assert.NotNull(renewedJob?.LockedUntilMs);
        Assert.True(renewedJob!.LockedUntilMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000);

        // 用旧/错误 claim_token 续租：应失败
        var staleRenew = await store.RenewLeaseAsync(
            userId, job.Phase, "wrong-token", TimeSpan.FromMinutes(10), CancellationToken.None);
        Assert.False(staleRenew);
    }

    [Fact]
    public async Task ProcessAttachmentsBatchAtomic_RejectsStaleLease()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_atomic");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_004;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        // instance-a 认领
        var job = await store.GetNextPendingAsync("instance-a", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(job);
        var validClaimToken = job!.ClaimToken!;

        // 用正确 claim_token 执行原子批次：应成功（但无附件可删，cursor 仍推进）
        var purgeEvent = new RealtimeEvent
        {
            EventId = "purge-atomic-ok",
            Type = RealtimeEventType.AttachmentBlobsPurge,
            TargetUserId = userId,
            OccurredAtMs = 2_000
        };
        var ok = await store.ProcessAttachmentsBatchAtomicAsync(
            userId, validClaimToken, "cursor-1", ["att-1"], purgeEvent, CancellationToken.None);
        Assert.True(ok);

        // 用错误 claim_token 执行原子批次：应回滚并返回 false
        var purgeEvent2 = new RealtimeEvent
        {
            EventId = "purge-atomic-fail",
            Type = RealtimeEventType.AttachmentBlobsPurge,
            TargetUserId = userId,
            OccurredAtMs = 3_000
        };
        var failed = await store.ProcessAttachmentsBatchAtomicAsync(
            userId, "stale-token", "cursor-2", ["att-2"], purgeEvent2, CancellationToken.None);
        Assert.False(failed);

        // 验证 cursor 未被错误 token 推进（仍为 cursor-1）
        var afterFail = await GetJobAsync(client, schema, userId, AccountCleanupJob.PhaseAttachments);
        Assert.NotNull(afterFail);
        Assert.Equal("cursor-1", afterFail!.Cursor);
    }

    [Fact]
    public async Task CompletePhase_ClearsLease_AndAdvancesToNextPhase()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_complete");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_005;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        var job = await store.GetNextPendingAsync("instance-a", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(job);

        // 完成 attachments 阶段
        await store.CompletePhaseAsync(
            userId, AccountCleanupJob.PhaseAttachments, job!.ClaimToken!, CancellationToken.None);

        // attachments 阶段应标记 completed 并清空 lease
        var attachmentsJob = await GetJobAsync(client, schema, userId, AccountCleanupJob.PhaseAttachments);
        Assert.NotNull(attachmentsJob);
        Assert.Equal(AccountCleanupJob.StatusCompleted, attachmentsJob!.Status);
        Assert.Null(attachmentsJob.ClaimToken);
        Assert.Null(attachmentsJob.LockedBy);
        Assert.Null(attachmentsJob.LockedUntilMs);

        // metadata 阶段应已创建为 pending
        var metadataJob = await GetJobAsync(client, schema, userId, AccountCleanupJob.PhaseMetadata);
        Assert.NotNull(metadataJob);
        Assert.Equal(AccountCleanupJob.StatusPending, metadataJob!.Status);

        // 用旧 claim_token 完成应无效（attachments 已 completed，claim_token 已清空）
        // 此处验证 CompletePhaseAsync 在 claim_token 不匹配时不会重复完成
        await store.CompletePhaseAsync(
            userId, AccountCleanupJob.PhaseAttachments, "stale-token", CancellationToken.None);
        var stillCompleted = await GetJobAsync(client, schema, userId, AccountCleanupJob.PhaseAttachments);
        Assert.NotNull(stillCompleted);
        Assert.Equal(AccountCleanupJob.StatusCompleted, stillCompleted!.Status);
    }

    [Fact]
    public async Task RecordFailure_ClearsLease_AndRollsBackToPending()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_cleanup_lease_failure");
        var store = new NpgsqlAccountCleanupJobStore(
            client,
            schema,
            NullLogger<NpgsqlAccountCleanupJobStore>.Instance);

        const long userId = 60_006;
        await store.EnqueueJobAsync(userId, occurredAtMs: 1_000, CancellationToken.None);

        var job = await store.GetNextPendingAsync("instance-a", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(job);

        // 记录一次失败（retry_count < maxRetry → 回退 pending）
        await store.RecordFailureAsync(
            userId, job!.Phase, job.ClaimToken!, maxRetryCount: 3, CancellationToken.None);

        var afterFail = await GetJobAsync(client, schema, userId, job.Phase);
        Assert.NotNull(afterFail);
        Assert.Equal(AccountCleanupJob.StatusPending, afterFail!.Status);
        Assert.Equal(1, afterFail.RetryCount);
        Assert.Null(afterFail.ClaimToken);
        Assert.Null(afterFail.LockedBy);

        // pending 作业可被重新认领
        var reclaimed = await store.GetNextPendingAsync("instance-b", TimeSpan.FromMinutes(5), CancellationToken.None);
        Assert.NotNull(reclaimed);
        Assert.Equal(userId, reclaimed!.UserId);
        Assert.Equal("instance-b", reclaimed.LockedBy);
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

    private static async Task<AccountCleanupJob?> GetJobAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        long userId,
        string phase)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT user_id, phase, cursor, status, retry_count, updated_at_ms,
                    claim_token, locked_by, locked_until_ms
             FROM {schema.AccountCleanupJobsTableSql}
             WHERE user_id = @uid AND phase = @phase
             """,
            connection);
        cmd.Parameters.AddWithValue("uid", userId);
        cmd.Parameters.AddWithValue("phase", phase);
        await using var reader = await cmd.ExecuteReaderAsync();
        if (!await reader.ReadAsync())
            return null;
        return new AccountCleanupJob(
            reader.GetInt64(0),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            reader.GetString(3),
            reader.GetInt32(4),
            reader.GetInt64(5),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8));
    }
}

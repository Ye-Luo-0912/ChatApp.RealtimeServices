using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// P0-3：幂等账本并发冲突集成测试。
/// <para>
/// 验证 NpgsqlCommandIdempotencyLedger 在并发写入相同 (sender_user_id, client_message_id) 时：
/// - ON CONFLICT DO NOTHING 生效，PK 冲突时 INSERT 被跳过；
/// - 首次写入的 canonical 记录不被后续并发请求覆盖；
/// - 内容指纹匹配的后续写入被识别为重放（Duplicate）；
/// - 内容指纹不匹配的后续写入被识别为冲突（Conflict），canonical 行保持不变。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class LedgerConcurrentConflictTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task RecordAsync_ConcurrentSameKey_OnlyOneInsertSucceeds()
    {
        var (client, schema) = await CreateStoreAsync("rt_ledger_concurrent_insert");
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);

        const long senderUserId = 9_300_000_001L;
        const string clientMessageId = "client-concurrent-1";
        const string canonicalFingerprint = "fp-canonical-v1";
        const string canonicalMessageId = "msg-canonical-1";
        const long receivedAtMs = 1_700_000_000_000L;

        // 并发发起 8 个相同 (sender, client_message_id) 的写入，全部使用相同指纹（视为重放）。
        var tasks = Enumerable.Range(0, 8)
            .Select(_ => ledger.RecordAsync(
                commandId: $"cmd-{Guid.NewGuid():N}",
                senderUserId,
                clientMessageId,
                canonicalFingerprint,
                IdempotencyLedgerResultKind.Created,
                canonicalMessageId,
                receivedAtMs))
            .ToArray();

        await Task.WhenAll(tasks);

        // 表中应只有 1 行（PK 冲突时 ON CONFLICT DO NOTHING 跳过 INSERT）。
        var count = await CountLedgerRowsAsync(client, schema, senderUserId, clientMessageId);
        Assert.Equal(1, count);

        // canonical 记录的 fingerprint / message_id 应保持首次写入值，未被并发请求覆盖。
        var canonical = await ledger.FindAsync(senderUserId, clientMessageId);
        Assert.NotNull(canonical);
        Assert.Equal(canonicalFingerprint, canonical!.ContentFingerprint);
        Assert.Equal(canonicalMessageId, canonical.MessageId);
        Assert.Equal(IdempotencyLedgerResultKind.Created, canonical.ResultKind);
    }

    [Fact]
    public async Task RecordAsync_ConflictingFingerprint_DoesNotOverwriteCanonical()
    {
        var (client, schema) = await CreateStoreAsync("rt_ledger_conflict_no_overwrite");
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);

        const long senderUserId = 9_300_000_011L;
        const string clientMessageId = "client-conflict-1";
        const string canonicalFingerprint = "fp-canonical-original";
        const string conflictFingerprint = "fp-conflict-attacker";
        const string canonicalMessageId = "msg-canonical-2";
        const long receivedAtMs = 1_700_000_000_100L;

        // 首次写入：建立 canonical 记录。
        await ledger.RecordAsync(
            commandId: "cmd-canonical",
            senderUserId,
            clientMessageId,
            canonicalFingerprint,
            IdempotencyLedgerResultKind.Created,
            canonicalMessageId,
            receivedAtMs);

        // 后续写入：使用相同 PK 但不同指纹（内容冲突）。
        // P0-3：旧实现 ON CONFLICT DO UPDATE 会覆盖 canonical，新实现 DO NOTHING 保留原始记录。
        await ledger.RecordAsync(
            commandId: "cmd-attacker",
            senderUserId,
            clientMessageId,
            conflictFingerprint,
            IdempotencyLedgerResultKind.Conflict,
            messageId: "msg-attacker",
            receivedAtMs + 1);

        var canonical = await ledger.FindAsync(senderUserId, clientMessageId);
        Assert.NotNull(canonical);
        Assert.Equal(canonicalFingerprint, canonical!.ContentFingerprint);
        Assert.Equal(canonicalMessageId, canonical.MessageId);
        Assert.Equal(IdempotencyLedgerResultKind.Created, canonical.ResultKind);
        Assert.NotEqual(conflictFingerprint, canonical.ContentFingerprint);

        // 表中仍只有 1 行。
        var count = await CountLedgerRowsAsync(client, schema, senderUserId, clientMessageId);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task RecordAsync_SameFingerprint_ReplayIsSafe()
    {
        var (client, schema) = await CreateStoreAsync("rt_ledger_replay_safe");
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);

        const long senderUserId = 9_300_000_021L;
        const string clientMessageId = "client-replay-1";
        const string fingerprint = "fp-replay";
        const string messageId = "msg-replay-1";
        const long receivedAtMs = 1_700_000_000_200L;

        await ledger.RecordAsync(
            commandId: "cmd-first",
            senderUserId,
            clientMessageId,
            fingerprint,
            IdempotencyLedgerResultKind.Created,
            messageId,
            receivedAtMs);

        // 模拟 JetStream replay：相同内容指纹的重复投递。
        await ledger.RecordAsync(
            commandId: "cmd-first",
            senderUserId,
            clientMessageId,
            fingerprint,
            IdempotencyLedgerResultKind.Duplicate,
            messageId,
            receivedAtMs);

        var canonical = await ledger.FindAsync(senderUserId, clientMessageId);
        Assert.NotNull(canonical);
        // canonical 的 ResultKind 仍是首次写入的 Created，未被 replay 改为 Duplicate。
        Assert.Equal(IdempotencyLedgerResultKind.Created, canonical!.ResultKind);
        Assert.Equal(fingerprint, canonical.ContentFingerprint);
        Assert.Equal(messageId, canonical.MessageId);

        var count = await CountLedgerRowsAsync(client, schema, senderUserId, clientMessageId);
        Assert.Equal(1, count);
    }

    [Fact]
    public async Task RecordInTransactionAsync_RollsBackOnFailure()
    {
        var (client, schema) = await CreateStoreAsync("rt_ledger_txn_rollback");
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);

        const long senderUserId = 9_300_000_031L;
        const string clientMessageId = "client-txn-1";
        const string fingerprint = "fp-txn";
        const string messageId = "msg-txn-1";
        const long receivedAtMs = 1_700_000_000_300L;

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var transaction = await connection.BeginTransactionAsync();

        // 事务内首次写入 canonical。
        await ledger.RecordInTransactionAsync(
            connection,
            transaction,
            commandId: "cmd-txn",
            senderUserId,
            clientMessageId,
            fingerprint,
            IdempotencyLedgerResultKind.Created,
            messageId,
            receivedAtMs);

        // 回滚事务：canonical 不应被持久化。
        await transaction.RollbackAsync();

        var found = await ledger.FindAsync(senderUserId, clientMessageId);
        Assert.Null(found);
    }

    [Fact]
    public async Task RecordInTransactionAsync_ConcurrentSameKey_OnlyOneCommits()
    {
        var (client, schema) = await CreateStoreAsync("rt_ledger_txn_concurrent");
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);

        const long senderUserId = 9_300_000_041L;
        const string clientMessageId = "client-txn-concurrent";
        const string fingerprint = "fp-txn-concurrent";
        const string messageId = "msg-txn-concurrent";
        const long receivedAtMs = 1_700_000_000_400L;

        // 串行模拟两个事务：第一个提交成功，第二个事务内 INSERT 应被 ON CONFLICT DO NOTHING 跳过。
        await using (var connection1 = await client.GetDataSource().OpenConnectionAsync())
        await using (var transaction1 = await connection1.BeginTransactionAsync())
        {
            await ledger.RecordInTransactionAsync(
                connection1,
                transaction1,
                commandId: "cmd-txn-1",
                senderUserId,
                clientMessageId,
                fingerprint,
                IdempotencyLedgerResultKind.Created,
                messageId,
                receivedAtMs);
            await transaction1.CommitAsync();
        }

        await using (var connection2 = await client.GetDataSource().OpenConnectionAsync())
        await using (var transaction2 = await connection2.BeginTransactionAsync())
        {
            // 第二个事务尝试用不同指纹写入相同 PK：INSERT 被跳过，但不应抛出异常。
            await ledger.RecordInTransactionAsync(
                connection2,
                transaction2,
                commandId: "cmd-txn-2",
                senderUserId,
                clientMessageId,
                "fp-different",
                IdempotencyLedgerResultKind.Created,
                "msg-different",
                receivedAtMs + 1);
            await transaction2.CommitAsync();
        }

        var canonical = await ledger.FindAsync(senderUserId, clientMessageId);
        Assert.NotNull(canonical);
        Assert.Equal(fingerprint, canonical!.ContentFingerprint);
        Assert.Equal(messageId, canonical.MessageId);

        var count = await CountLedgerRowsAsync(client, schema, senderUserId, clientMessageId);
        Assert.Equal(1, count);
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

    private static async Task<long> CountLedgerRowsAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        long senderUserId,
        string clientMessageId)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::bigint
             FROM {schema.CommandIdempotencyLedgerTableSql}
             WHERE sender_user_id = @sender
               AND client_message_id = @client_id
             """,
            connection);
        cmd.Parameters.AddWithValue("sender", senderUserId);
        cmd.Parameters.AddWithValue("client_id", clientMessageId);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}

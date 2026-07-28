using System.Data.Common;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// LongTerm-1：PostgreSQL 独立命令幂等性账本。
/// <para>
/// PK = (sender_user_id, client_message_id)，与 messages 唯一索引一致。
/// 消息行被 retention GC 或账号删除清理后，账本仍保留命令处理结果，
/// 防止 JetStream replay 将旧命令当作新消息重新写入。
/// </para>
/// <para>
/// P0-3：RecordAsync 使用 ON CONFLICT DO NOTHING RETURNING 保护 canonical 记录。
/// 首次写入记录 Created；后续重复投递不再覆盖已有 canonical 行——
/// 通过 RETURNING 是否有行区分首次写入与已存在，已存在时读取 canonical 的
/// content_fingerprint 判断是重放（指纹匹配）还是冲突（指纹不一致）。
/// 旧实现使用 ON CONFLICT DO UPDATE 会被并发请求用不同内容覆盖原始 canonical 记录。
/// </para>
/// </summary>
public sealed class NpgsqlCommandIdempotencyLedger(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema,
    ILogger<NpgsqlCommandIdempotencyLedger> logger) : ICommandIdempotencyLedger
{
    public async Task<IdempotencyLedgerEntry?> FindAsync(
        long senderUserId,
        string clientMessageId,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || senderUserId <= 0 || string.IsNullOrEmpty(clientMessageId))
            return null;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        return await ReadCanonicalAsync(connection, senderUserId, clientMessageId, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// P0-3：读取已有的 canonical 账本记录，用于与当前请求的 fingerprint 比较。
    /// <para>
    /// 在 RecordAsync 改用 ON CONFLICT DO NOTHING 后，当 INSERT 被跳过时调用此方法
    /// 读取已有 canonical 行，以区分真重放（指纹匹配）与内容冲突（指纹不一致）。
    /// 与 <see cref="FindAsync"/> 等价，提供更明确的语义命名供直接调用方使用。
    /// </para>
    /// </summary>
    public async Task<IdempotencyLedgerEntry?> GetCanonicalAsync(
        long senderUserId,
        string clientMessageId,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || senderUserId <= 0 || string.IsNullOrEmpty(clientMessageId))
            return null;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        return await ReadCanonicalAsync(connection, senderUserId, clientMessageId, ct)
            .ConfigureAwait(false);
    }

    public async Task RecordAsync(
        string commandId,
        long senderUserId,
        string clientMessageId,
        string contentFingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || senderUserId <= 0 || string.IsNullOrEmpty(clientMessageId))
            return;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        try
        {
            await ExecuteRecordAsync(
                connection,
                transaction: null,
                commandId,
                senderUserId,
                clientMessageId,
                contentFingerprint,
                kind,
                messageId,
                receivedAtMs,
                ct).ConfigureAwait(false);
        }
        catch (Exception ex) when (ex is not OperationCanceledException)
        {
            // LongTerm-1：账本写入失败不阻断主流程。消息已持久化，账本缺失仅意味着
            // retention GC 后旧命令可能"复活"——但 tombstone 检查会拒绝已注销用户，
            // 且 retention GC 周期（默认 60s）远大于此 crash 窗口。
            logger.LogWarning(
                ex,
                "幂等账本写入失败（不阻断主流程）。sender={SenderUserId}; clientMessageId={ClientMessageId}; kind={Kind}",
                senderUserId,
                clientMessageId,
                kind);
        }
    }

    /// <summary>
    /// Perf-3：在指定连接和事务内查询幂等账本，复用调用方事务，避免独立连接获取。
    /// </summary>
    public async Task<IdempotencyLedgerEntry?> FindInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        long senderUserId,
        string clientMessageId,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || senderUserId <= 0 || string.IsNullOrEmpty(clientMessageId))
            return null;

        // Abstractions 层使用 System.Data.Common 抽象；Npgsql 实现层向下转型以复用 SQL Helper。
        var npgsqlConnection = (NpgsqlConnection)connection;
        return await ReadCanonicalAsync(npgsqlConnection, senderUserId, clientMessageId, ct)
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Perf-3：在指定连接和事务内记录幂等账本，复用调用方事务。
    /// <para>
    /// 与 <see cref="RecordAsync"/> 不同：事务内失败不再被吞掉——应让整个事务回滚，
    /// 保证 ledger 与 messages 行的原子性。这是更正确的行为：原 best-effort 路径仅作为
    /// 事务外回填，事务内路径必须与消息写入同生共死。
    /// </para>
    /// </summary>
    public async Task RecordInTransactionAsync(
        DbConnection connection,
        DbTransaction transaction,
        string commandId,
        long senderUserId,
        string clientMessageId,
        string contentFingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured || senderUserId <= 0 || string.IsNullOrEmpty(clientMessageId))
            return;

        var npgsqlConnection = (NpgsqlConnection)connection;
        var npgsqlTransaction = (NpgsqlTransaction)transaction;

        // Perf-3：事务内失败向上抛出，让调用方回滚整个事务。
        await ExecuteRecordAsync(
            npgsqlConnection,
            npgsqlTransaction,
            commandId,
            senderUserId,
            clientMessageId,
            contentFingerprint,
            kind,
            messageId,
            receivedAtMs,
            ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 共享 INSERT 实现：RecordAsync（无事务）与 RecordInTransactionAsync（有事务）复用。
    /// </summary>
    private async Task ExecuteRecordAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction? transaction,
        string commandId,
        long senderUserId,
        string clientMessageId,
        string contentFingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct)
    {
        // P0-3：使用 ON CONFLICT DO NOTHING RETURNING 保护 canonical 记录不被覆盖。
        // 旧实现使用 ON CONFLICT DO UPDATE 会覆盖已有 canonical 的 content_fingerprint /
        // result_kind / message_id，两个并发请求使用相同 ClientMessageId、不同内容时，
        // 后到请求会覆盖原始 canonical 记录，破坏幂等性判定的权威性。
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.CommandIdempotencyLedgerTableSql}
                 (sender_user_id, client_message_id, command_id, content_fingerprint,
                  result_kind, message_id, received_at_ms)
             VALUES (@sender_user_id, @client_message_id, @command_id, @content_fingerprint,
                     @result_kind, @message_id, @received_at_ms)
             ON CONFLICT (sender_user_id, client_message_id) DO NOTHING
             RETURNING command_id, content_fingerprint, result_kind, message_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("client_message_id", clientMessageId);
        command.Parameters.AddWithValue("command_id", commandId);
        command.Parameters.AddWithValue("content_fingerprint", contentFingerprint);
        command.Parameters.AddWithValue("result_kind", (byte)kind);
        command.Parameters.AddWithValue(
            "message_id",
            (object?)messageId ?? DBNull.Value);
        command.Parameters.AddWithValue("received_at_ms", receivedAtMs);

        // P0-3：reader 必须在 ReadCanonicalAsync 之前释放，否则同一连接上会触发
        // NpgsqlOperationInProgressException（INSERT...RETURNING 0 行时 reader 仍占用连接）。
        {
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                // RETURNING 有行：首次写入成功（canonical Created），无需进一步处理。
                logger.LogDebug(
                    "幂等账本首次写入成功（canonical）。sender={SenderUserId}; clientMessageId={ClientMessageId}; kind={Kind}",
                    senderUserId,
                    clientMessageId,
                    kind);
                return;
            }
        }

        // RETURNING 无行：reader 已释放，可安全读取已存在的 canonical 记录。
        // 读取已有 canonical 进行 fingerprint 比较，判断是 Replay 还是 Conflict，
        // 但无论结果如何都不覆盖 canonical 行。
        var canonical = await ReadCanonicalAsync(connection, senderUserId, clientMessageId, ct)
            .ConfigureAwait(false);
        if (canonical is null)
        {
            // 理论上不会发生（INSERT 被跳过意味着行已存在）。防御性日志。
            logger.LogWarning(
                "幂等账本 INSERT 被跳过但 canonical 读取为空。sender={SenderUserId}; clientMessageId={ClientMessageId}",
                senderUserId,
                clientMessageId);
            return;
        }

        if (string.Equals(canonical.ContentFingerprint, contentFingerprint, StringComparison.Ordinal))
        {
            // 幂等重放：内容指纹匹配，canonical 保持不变。
            logger.LogDebug(
                "幂等账本命中（重放，不覆盖 canonical）。sender={SenderUserId}; clientMessageId={ClientMessageId}; canonicalKind={CanonicalKind}",
                senderUserId,
                clientMessageId,
                canonical.ResultKind);
        }
        else
        {
            // 内容冲突：指纹不一致。不覆盖 canonical，仅记录审计日志。
            // P0-3：旧实现此处会覆盖 canonical，导致并发请求用不同内容污染原始记录。
            logger.LogWarning(
                "幂等账本冲突（不覆盖 canonical）。sender={SenderUserId}; clientMessageId={ClientMessageId}; " +
                "canonicalFingerprint={CanonicalFingerprint}; incomingFingerprint={IncomingFingerprint}; " +
                "canonicalKind={CanonicalKind}; incomingKind={IncomingKind}; canonicalMessageId={CanonicalMessageId}",
                senderUserId,
                clientMessageId,
                canonical.ContentFingerprint,
                contentFingerprint,
                canonical.ResultKind,
                kind,
                canonical.MessageId);
        }
    }

    /// <summary>
    /// 使用已有连接读取 canonical 记录，避免 RecordAsync 内重复开连接。
    /// </summary>
    private async Task<IdempotencyLedgerEntry?> ReadCanonicalAsync(
        NpgsqlConnection connection,
        long senderUserId,
        string clientMessageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT command_id, content_fingerprint, result_kind, message_id, received_at_ms
             FROM {databaseSchema.CommandIdempotencyLedgerTableSql}
             WHERE sender_user_id = @sender_user_id
               AND client_message_id = @client_message_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("client_message_id", clientMessageId);

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new IdempotencyLedgerEntry(
            SenderUserId: senderUserId,
            ClientMessageId: clientMessageId,
            CommandId: reader.GetString(0),
            ContentFingerprint: reader.GetString(1),
            ResultKind: (IdempotencyLedgerResultKind)reader.GetByte(2),
            MessageId: reader.IsDBNull(3) ? null : reader.GetString(3),
            ReceivedAtMs: reader.GetInt64(4));
    }

    public async Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 10_000);
        if (!databaseClient.IsConfigured)
            return 0;

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {databaseSchema.CommandIdempotencyLedgerTableSql}
             WHERE (sender_user_id, client_message_id) IN (
                 SELECT sender_user_id, client_message_id
                 FROM {databaseSchema.CommandIdempotencyLedgerTableSql}
                 WHERE received_at_ms < @cutoff
                 LIMIT @batch_size
                 FOR UPDATE SKIP LOCKED
             );
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", cutoffMs);
        command.Parameters.AddWithValue("batch_size", batchSize);

        var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (deleted > 0)
        {
            logger.LogDebug(
                "幂等账本 GC 已清理 {Count} 条过期记录。cutoff={Cutoff}",
                deleted,
                cutoffMs);
        }
        return deleted;
    }
}

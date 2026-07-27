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
/// RecordAsync 使用 ON CONFLICT DO UPDATE：首次写入记录 Created；
/// 后续重复投递更新为最新结果（Duplicate / Conflict）。content_fingerprint 用于
/// 区分真重放与内容冲突，由调用方（DefaultIncomingMessageProcessor）计算。
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

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {databaseSchema.CommandIdempotencyLedgerTableSql}
                 (sender_user_id, client_message_id, command_id, content_fingerprint,
                  result_kind, message_id, received_at_ms)
             VALUES (@sender_user_id, @client_message_id, @command_id, @content_fingerprint,
                     @result_kind, @message_id, @received_at_ms)
             ON CONFLICT (sender_user_id, client_message_id) DO UPDATE
             SET command_id = EXCLUDED.command_id,
                 content_fingerprint = EXCLUDED.content_fingerprint,
                 result_kind = EXCLUDED.result_kind,
                 message_id = EXCLUDED.message_id,
                 received_at_ms = EXCLUDED.received_at_ms;
             """,
            connection);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("client_message_id", clientMessageId);
        command.Parameters.AddWithValue("command_id", commandId);
        command.Parameters.AddWithValue("content_fingerprint", contentFingerprint);
        command.Parameters.AddWithValue("result_kind", (byte)kind);
        command.Parameters.AddWithValue(
            "message_id",
            (object?)messageId ?? DBNull.Value);
        command.Parameters.AddWithValue("received_at_ms", receivedAtMs);

        try
        {
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// Hard-deletes aged message rows under a session advisory lock. Attachments are left Bound
/// (orphan blob GC is owned elsewhere).
/// </summary>
public sealed class NpgsqlRealtimeMessageRetentionStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema databaseSchema,
    ILogger<NpgsqlRealtimeMessageRetentionStore> logger) : IRealtimeMessageRetentionStore
{
    /// <summary>Distinct from migration lock (<see cref="Migrations.RealtimeSchemaMigrationRunner.AdvisoryLockKey"/>).</summary>
    public const long AdvisoryLockKey = 0x4D53_4752_4554_4E01L; // "MSGRETN\x01"

    public async Task<MessageRetentionPurgeBatchResult> TryPurgeBatchAsync(
        long cutoffReceivedAtMs,
        int batchSize,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 10_000);
        if (!databaseClient.IsConfigured)
        {
            return new MessageRetentionPurgeBatchResult(true, 0, 0);
        }

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        if (!await TryAcquireLockAsync(connection, ct).ConfigureAwait(false))
        {
            return new MessageRetentionPurgeBatchResult(
                LockAcquired: false,
                DeletedCount: 0,
                ConversationsTipRepaired: 0);
        }

        try
        {
            await using var transaction = await connection.BeginTransactionAsync(ct)
                .ConfigureAwait(false);

            var deletedIds = new List<string>(batchSize);
            var affectedConversations = new HashSet<string>(StringComparer.Ordinal);

            await using (var select = new NpgsqlCommand(
                             $"""
                              SELECT message_id, conversation_id
                              FROM {databaseSchema.MessagesTableSql}
                              WHERE received_at_ms < @cutoff
                              ORDER BY received_at_ms, message_id
                              LIMIT @batch_size
                              FOR UPDATE SKIP LOCKED;
                              """,
                             connection,
                             transaction))
            {
                select.Parameters.AddWithValue("cutoff", cutoffReceivedAtMs);
                select.Parameters.AddWithValue("batch_size", batchSize);
                await using var reader = await select.ExecuteReaderAsync(ct).ConfigureAwait(false);
                while (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    deletedIds.Add(reader.GetString(0));
                    if (!reader.IsDBNull(1))
                    {
                        var conversationId = reader.GetString(1);
                        if (!string.IsNullOrWhiteSpace(conversationId))
                            affectedConversations.Add(conversationId);
                    }
                }
            }

            if (deletedIds.Count == 0)
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageRetentionPurgeBatchResult(true, 0, 0);
            }

            await DeleteByMessageIdsAsync(
                    connection,
                    transaction,
                    databaseSchema.MessageReactionsTableSql,
                    deletedIds,
                    ct)
                .ConfigureAwait(false);

            await DeleteByMessageIdsAsync(
                    connection,
                    transaction,
                    databaseSchema.MessageMutationRequestsTableSql,
                    deletedIds,
                    ct)
                .ConfigureAwait(false);

            await using (var deleteMessages = new NpgsqlCommand(
                             $"""
                              DELETE FROM {databaseSchema.MessagesTableSql}
                              WHERE message_id = ANY(@message_ids);
                              """,
                             connection,
                             transaction))
            {
                deleteMessages.Parameters.AddWithValue("message_ids", deletedIds.ToArray());
                await deleteMessages.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            var repaired = 0;
            if (affectedConversations.Count > 0)
            {
                repaired = await RepairConversationTipsAsync(
                        connection,
                        transaction,
                        affectedConversations,
                        ct)
                    .ConfigureAwait(false);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);

            logger.LogDebug(
                "Message retention purged batch. Deleted={Deleted}; TipsRepaired={Repaired}; Cutoff={Cutoff}",
                deletedIds.Count,
                repaired,
                cutoffReceivedAtMs);

            return new MessageRetentionPurgeBatchResult(
                LockAcquired: true,
                DeletedCount: deletedIds.Count,
                ConversationsTipRepaired: repaired);
        }
        finally
        {
            await ReleaseLockAsync(connection, ct).ConfigureAwait(false);
        }
    }

    public async Task<MessageRetentionPurgeableStats> GetPurgeableStatsAsync(
        long cutoffReceivedAtMs,
        CancellationToken ct = default)
    {
        if (!databaseClient.IsConfigured)
            return new MessageRetentionPurgeableStats(0, null);

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::bigint, MIN(received_at_ms)
             FROM {databaseSchema.MessagesTableSql}
             WHERE received_at_ms < @cutoff;
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", cutoffReceivedAtMs);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return new MessageRetentionPurgeableStats(0, null);

        var count = reader.GetInt64(0);
        long? oldest = reader.IsDBNull(1) ? null : reader.GetInt64(1);
        return new MessageRetentionPurgeableStats(count, oldest);
    }

    private async Task<int> RepairConversationTipsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var repaired = 0;
        foreach (var conversationId in conversationIds)
        {
            string? tipMessageId = null;
            string? tipPreview = null;
            long? tipAtMs = null;
            long? tipSender = null;

            await using (var tipCmd = new NpgsqlCommand(
                             $"""
                              SELECT message_id, content, received_at_ms, sender_user_id, recalled_at_ms
                              FROM {databaseSchema.MessagesTableSql}
                              WHERE conversation_id = @conversation_id
                              ORDER BY received_at_ms DESC, message_id DESC
                              LIMIT 1;
                              """,
                             connection,
                             transaction))
            {
                tipCmd.Parameters.AddWithValue("conversation_id", conversationId);
                await using var reader = await tipCmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    tipMessageId = reader.GetString(0);
                    var content = reader.GetString(1);
                    tipAtMs = reader.GetInt64(2);
                    tipSender = reader.GetInt64(3);
                    var recalled = !reader.IsDBNull(4);
                    tipPreview = recalled
                        ? "消息已撤回"
                        : ConversationId.CreatePreview(content);
                }
            }

            await using (var updateConv = new NpgsqlCommand(
                             $"""
                              UPDATE {databaseSchema.ConversationsTableSql}
                              SET last_message_id = @message_id,
                                  last_message_preview = @preview,
                                  last_message_at_ms = @at_ms,
                                  last_sender_user_id = @sender_user_id,
                                  updated_at_ms = @now
                              WHERE conversation_id = @conversation_id
                                AND (
                                     last_message_id IS DISTINCT FROM @message_id
                                  OR last_message_at_ms IS DISTINCT FROM @at_ms
                                  OR last_message_preview IS DISTINCT FROM @preview
                                  OR last_sender_user_id IS DISTINCT FROM @sender_user_id
                                );
                              """,
                             connection,
                             transaction))
            {
                updateConv.Parameters.AddWithValue("conversation_id", conversationId);
                updateConv.Parameters.AddWithValue("message_id", (object?)tipMessageId ?? DBNull.Value);
                updateConv.Parameters.AddWithValue("preview", (object?)tipPreview ?? DBNull.Value);
                updateConv.Parameters.AddWithValue("at_ms", (object?)tipAtMs ?? DBNull.Value);
                updateConv.Parameters.AddWithValue("sender_user_id", (object?)tipSender ?? DBNull.Value);
                updateConv.Parameters.AddWithValue("now", now);
                repaired += await updateConv.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }

            // Keep member sort key aligned with conversation tip (or clear when empty).
            await using (var updateMembers = new NpgsqlCommand(
                             $"""
                              UPDATE {databaseSchema.ConversationMembersTableSql}
                              SET last_message_at_ms = @at_ms
                              WHERE conversation_id = @conversation_id
                                AND last_message_at_ms IS DISTINCT FROM @at_ms;
                              """,
                             connection,
                             transaction))
            {
                updateMembers.Parameters.AddWithValue("conversation_id", conversationId);
                updateMembers.Parameters.AddWithValue("at_ms", (object?)tipAtMs ?? DBNull.Value);
                await updateMembers.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            }
        }

        return repaired;
    }

    private static async Task DeleteByMessageIdsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string tableSql,
        IReadOnlyList<string> messageIds,
        CancellationToken ct)
    {
        try
        {
            await using var command = new NpgsqlCommand(
                $"""
                 DELETE FROM {tableSql}
                 WHERE message_id = ANY(@message_ids);
                 """,
                connection,
                transaction);
            command.Parameters.AddWithValue("message_ids", messageIds.ToArray());
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01")
        {
            // Table may be absent on partial-migration test schemas.
        }
    }

    private static async Task<bool> TryAcquireLockAsync(
        NpgsqlConnection connection,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_try_advisory_lock(@key);",
            connection);
        command.Parameters.AddWithValue("key", AdvisoryLockKey);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is true;
    }

    private static async Task ReleaseLockAsync(
        NpgsqlConnection connection,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            "SELECT pg_advisory_unlock(@key);",
            connection);
        command.Parameters.AddWithValue("key", AdvisoryLockKey);
        await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
    }
}

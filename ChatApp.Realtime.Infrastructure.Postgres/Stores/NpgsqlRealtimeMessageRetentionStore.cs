using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// Hard-deletes aged message rows under a session advisory lock.
/// Bound attachments on purged messages are unbound → Abandoned and scheduled for blob
/// cleanup via <c>AttachmentBlobsPurge</c> outbox (Server consumer). Confirmed-but-unbound
/// uploads are left untouched.
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

            // Schedule blob cleanup before message delete so a crash after commit still has
            // Abandoned rows + outbox (or neither). Confirmed-unbound uploads are not selected.
            var (attachmentsAbandoned, purgeEventsEnqueued) = await UnbindAndScheduleAttachmentCleanupAsync(
                    connection,
                    transaction,
                    deletedIds,
                    ct)
                .ConfigureAwait(false);

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
            var unreadRepaired = 0;
            if (affectedConversations.Count > 0)
            {
                repaired = await RepairConversationTipsAsync(
                        connection,
                        transaction,
                        affectedConversations,
                        ct)
                    .ConfigureAwait(false);
                unreadRepaired = await RepairUnreadCountsAsync(
                        connection,
                        transaction,
                        affectedConversations,
                        ct)
                    .ConfigureAwait(false);
            }

            await transaction.CommitAsync(ct).ConfigureAwait(false);

            logger.LogDebug(
                "Message retention purged batch. Deleted={Deleted}; TipsRepaired={Repaired}; " +
                "AttachmentsAbandoned={Abandoned}; PurgeEvents={PurgeEvents}; UnreadRepaired={Unread}; Cutoff={Cutoff}",
                deletedIds.Count,
                repaired,
                attachmentsAbandoned,
                purgeEventsEnqueued,
                unreadRepaired,
                cutoffReceivedAtMs);

            return new MessageRetentionPurgeBatchResult(
                LockAcquired: true,
                DeletedCount: deletedIds.Count,
                ConversationsTipRepaired: repaired,
                AttachmentsAbandoned: attachmentsAbandoned,
                AttachmentPurgeEventsEnqueued: purgeEventsEnqueued,
                MembersUnreadRepaired: unreadRepaired);
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

    /// <summary>
    /// Unbinds Bound attachments on purged messages (Abandoned + clear message/conversation),
    /// then enqueues chunked <see cref="RealtimeEventType.AttachmentBlobsPurge"/> for Server blob GC.
    /// Does not touch Confirmed/Ticketed unbound uploads.
    /// </summary>
    private async Task<(int Abandoned, int PurgeEvents)> UnbindAndScheduleAttachmentCleanupAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<string> messageIds,
        CancellationToken ct)
    {
        List<(string AttachmentId, string ObjectKey, long UploaderUserId)> abandoned;
        try
        {
            await using var update = new NpgsqlCommand(
                $"""
                 UPDATE {databaseSchema.AttachmentsTableSql}
                 SET status = @abandoned,
                     message_id = NULL,
                     conversation_id = NULL
                 WHERE message_id = ANY(@message_ids)
                   AND status = @bound
                 RETURNING attachment_id, object_key, uploader_user_id;
                 """,
                connection,
                transaction);
            update.Parameters.AddWithValue("abandoned", (short)AttachmentStatus.Abandoned);
            update.Parameters.AddWithValue("bound", (short)AttachmentStatus.Bound);
            update.Parameters.AddWithValue("message_ids", messageIds.ToArray());

            abandoned = new List<(string, string, long)>();
            await using var reader = await update.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                var id = reader.GetString(0);
                var key = reader.GetString(1);
                var uploader = reader.GetInt64(2);
                if (!string.IsNullOrWhiteSpace(id) && !string.IsNullOrWhiteSpace(key))
                    abandoned.Add((id, key, uploader));
            }
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01")
        {
            // Attachments table may be absent on partial-migration test schemas.
            return (0, 0);
        }

        if (abandoned.Count == 0)
            return (0, 0);

        // Stable batch token so chunk EventIds are idempotent for this message set.
        var batchToken = Convert.ToHexStringLower(
            SHA256.HashData(Encoding.UTF8.GetBytes(
                string.Join('\n', messageIds.OrderBy(id => id, StringComparer.Ordinal)))));

        var events = new List<RealtimeEvent>();
        foreach (var group in abandoned.GroupBy(a => a.UploaderUserId))
        {
            var keys = group
                .Select(a => a.ObjectKey)
                .Distinct(StringComparer.Ordinal)
                .ToArray();
            var chunkSize = DefaultUserAccountDeletedProcessor.AttachmentPurgeChunkSize;
            var chunkCount = (keys.Length + chunkSize - 1) / chunkSize;
            for (var i = 0; i < chunkCount; i++)
            {
                var chunk = keys.Skip(i * chunkSize).Take(chunkSize).ToArray();
                var cleanupEventId = $"msgret:{batchToken}:{group.Key}";
                events.Add(new RealtimeEvent
                {
                    EventId = AttachmentEventIdFactory.CreateAttachmentBlobsPurgeEventId(
                        cleanupEventId,
                        i),
                    Type = RealtimeEventType.AttachmentBlobsPurge,
                    TargetUserId = group.Key,
                    ActorUserId = group.Key,
                    OccurredAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                    PayloadJson = JsonSerializer.Serialize(
                        new AttachmentBlobsPurgePayload
                        {
                            UserId = group.Key,
                            ObjectKeys = chunk,
                            ChunkIndex = i,
                            ChunkCount = chunkCount
                        },
                        RealtimeJsonSerializerContext.Default.AttachmentBlobsPurgePayload)
                });
            }
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
        return (abandoned.Count, events.Count);
    }

    private async Task<int> RepairConversationTipsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct)
        => await ConversationProjectionRepair.RepairConversationTipsAsync(
            connection, transaction, databaseSchema, conversationIds, ct).ConfigureAwait(false);

    /// <summary>
    /// Recounts <c>unread_count</c> from messages still present after the member's last-read
    /// watermark (clamped to non-negative / max tracked). Silent — no UnreadCountChanged fanout.
    /// </summary>
    private async Task<int> RepairUnreadCountsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyCollection<string> conversationIds,
        CancellationToken ct)
        => await ConversationProjectionRepair.RepairUnreadCountsAsync(
            connection, transaction, databaseSchema, conversationIds, ct).ConfigureAwait(false);

    private async Task InsertOutboxManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IReadOnlyList<RealtimeEvent> events,
        CancellationToken ct)
    {
        if (events.Count == 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand { Connection = connection, Transaction = transaction };
        var values = new List<string>(events.Count);
        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            values.Add(
                $"(@event_id_{i}, @payload_json_{i}, @target_user_id_{i}, @event_type_{i}, @status, @created_at_ms, @next_attempt_at_ms, 0)");
            command.Parameters.AddWithValue($"event_id_{i}", evt.EventId);
            command.Parameters.AddWithValue(
                $"payload_json_{i}",
                JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
            command.Parameters.AddWithValue($"target_user_id_{i}", evt.TargetUserId);
            command.Parameters.AddWithValue($"event_type_{i}", (short)evt.Type);
        }

        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        command.CommandText =
            $"""
             INSERT INTO {databaseSchema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES
                 {string.Join(",\n                 ", values)}
             ON CONFLICT (event_id) DO NOTHING;
             """;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

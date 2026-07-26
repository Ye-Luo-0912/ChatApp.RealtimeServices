using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeReactionStore : IRealtimeReactionStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeReactionStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<MessageReactionPersistResult> AddAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        MessageReactionOptions options,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(emoji);
        ArgumentException.ThrowIfNullOrWhiteSpace(actorSessionId);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(actorUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(occurredAtMs);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var access = await TryLockMessageAccessAsync(
                connection,
                transaction,
                messageId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (access is null)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotFound,
                messageId);
        }

        if (access.RecalledAtMs is not null)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.AlreadyRecalled,
                messageId,
                access.ConversationId,
                emoji);
        }

        if (!access.IsAllowed)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed,
                messageId,
                access.ConversationId,
                emoji);
        }

        var exists = await ReactionExistsAsync(
                connection,
                transaction,
                messageId,
                actorUserId,
                emoji,
                ct)
            .ConfigureAwait(false);
        if (exists)
        {
            var existingCount = await CountEmojiAsync(
                    connection,
                    transaction,
                    messageId,
                    emoji,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                messageId,
                access.ConversationId,
                emoji,
                occurredAtMs,
                existingCount);
        }

        var userCount = await CountUserReactionsAsync(
                connection,
                transaction,
                messageId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (userCount >= options.MaxReactionsPerUserPerMessage)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.LimitExceeded,
                messageId,
                access.ConversationId,
                emoji);
        }

        var emojiExists = await EmojiExistsOnMessageAsync(
                connection,
                transaction,
                messageId,
                emoji,
                ct)
            .ConfigureAwait(false);
        if (!emojiExists)
        {
            var distinct = await CountDistinctEmojisAsync(
                    connection,
                    transaction,
                    messageId,
                    ct)
                .ConfigureAwait(false);
            if (distinct >= options.MaxDistinctEmojisPerMessage)
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                return new MessageReactionPersistResult(
                    MessageReactionPersistStatus.LimitExceeded,
                    messageId,
                    access.ConversationId,
                    emoji);
            }
        }

        await using (var insert = new NpgsqlCommand(
                         $"""
                          INSERT INTO {_databaseSchema.MessageReactionsTableSql}
                              (message_id, user_id, emoji, created_at_ms)
                          VALUES
                              (@message_id, @user_id, @emoji, @created_at_ms)
                          ON CONFLICT (message_id, user_id, emoji) DO NOTHING;
                          """,
                         connection,
                         transaction))
        {
            insert.Parameters.AddWithValue("message_id", messageId);
            insert.Parameters.AddWithValue("user_id", actorUserId);
            insert.Parameters.AddWithValue("emoji", emoji);
            insert.Parameters.AddWithValue("created_at_ms", occurredAtMs);
            await insert.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await BumpChangedAtAsync(
                connection,
                transaction,
                messageId,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        var emojiCount = await CountEmojiAsync(
                connection,
                transaction,
                messageId,
                emoji,
                ct)
            .ConfigureAwait(false);

        await InsertReactionEventsAsync(
                connection,
                transaction,
                added: true,
                messageId,
                access.ConversationId,
                actorUserId,
                actorSessionId,
                access.SenderUserId,
                access.ReceiverUserId,
                emoji,
                emojiCount,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return new MessageReactionPersistResult(
            MessageReactionPersistStatus.Applied,
            messageId,
            access.ConversationId,
            emoji,
            occurredAtMs,
            emojiCount);
    }

    public async Task<MessageReactionPersistResult> RemoveAsync(
        string messageId,
        long actorUserId,
        string actorSessionId,
        string emoji,
        long occurredAtMs,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);
        ArgumentException.ThrowIfNullOrWhiteSpace(emoji);
        ArgumentException.ThrowIfNullOrWhiteSpace(actorSessionId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(actorUserId);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(occurredAtMs);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        var access = await TryLockMessageAccessAsync(
                connection,
                transaction,
                messageId,
                actorUserId,
                ct)
            .ConfigureAwait(false);
        if (access is null)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotFound,
                messageId);
        }

        if (access.RecalledAtMs is not null)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.AlreadyRecalled,
                messageId,
                access.ConversationId,
                emoji);
        }

        if (!access.IsAllowed)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.NotAllowed,
                messageId,
                access.ConversationId,
                emoji);
        }

        int deleted;
        await using (var delete = new NpgsqlCommand(
                         $"""
                          DELETE FROM {_databaseSchema.MessageReactionsTableSql}
                          WHERE message_id = @message_id
                            AND user_id = @user_id
                            AND emoji = @emoji;
                          """,
                         connection,
                         transaction))
        {
            delete.Parameters.AddWithValue("message_id", messageId);
            delete.Parameters.AddWithValue("user_id", actorUserId);
            delete.Parameters.AddWithValue("emoji", emoji);
            deleted = await delete.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        var emojiCount = await CountEmojiAsync(
                connection,
                transaction,
                messageId,
                emoji,
                ct)
            .ConfigureAwait(false);

        if (deleted == 0)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new MessageReactionPersistResult(
                MessageReactionPersistStatus.Unchanged,
                messageId,
                access.ConversationId,
                emoji,
                occurredAtMs,
                emojiCount);
        }

        await BumpChangedAtAsync(
                connection,
                transaction,
                messageId,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        await InsertReactionEventsAsync(
                connection,
                transaction,
                added: false,
                messageId,
                access.ConversationId,
                actorUserId,
                actorSessionId,
                access.SenderUserId,
                access.ReceiverUserId,
                emoji,
                emojiCount,
                occurredAtMs,
                ct)
            .ConfigureAwait(false);

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        return new MessageReactionPersistResult(
            MessageReactionPersistStatus.Applied,
            messageId,
            access.ConversationId,
            emoji,
            occurredAtMs,
            emojiCount);
    }

    public async Task<IReadOnlyList<MessageReactionRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(messageIds);
        if (messageIds.Count == 0)
            return [];

        var ids = messageIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (ids.Length == 0)
            return [];

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT message_id, user_id, emoji, created_at_ms
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = ANY(@message_ids)
             ORDER BY message_id, created_at_ms, user_id, emoji;
             """,
            connection);
        var param = command.Parameters.Add("message_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        param.Value = ids;

        var rows = new List<MessageReactionRecord>(ids.Length);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new MessageReactionRecord
            {
                MessageId = reader.GetString(0),
                UserId = reader.GetInt64(1),
                Emoji = reader.GetString(2),
                CreatedAtMs = reader.GetInt64(3)
            });
        }

        return rows;
    }

    private async Task<MessageAccess?> TryLockMessageAccessAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long actorUserId,
        CancellationToken ct)
    {
        long senderUserId;
        long receiverUserId;
        string? conversationId;
        long? recalledAtMs;

        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT sender_user_id, receiver_user_id, conversation_id, recalled_at_ms
                          FROM {_databaseSchema.MessagesTableSql}
                          WHERE message_id = @message_id
                          FOR UPDATE
                          """,
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", messageId);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return null;

            senderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            conversationId = reader.IsDBNull(2) ? null : reader.GetString(2);
            recalledAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
        }

        var isParticipant = actorUserId == senderUserId || actorUserId == receiverUserId;
        var isMember = isParticipant;
        if (!isMember && !string.IsNullOrWhiteSpace(conversationId))
        {
            await using var memberCmd = new NpgsqlCommand(
                $"""
                 SELECT 1
                 FROM {_databaseSchema.ConversationMembersTableSql}
                 WHERE conversation_id = @conversation_id
                   AND user_id = @user_id
                 LIMIT 1;
                 """,
                connection,
                transaction);
            memberCmd.Parameters.AddWithValue("conversation_id", conversationId);
            memberCmd.Parameters.AddWithValue("user_id", actorUserId);
            var scalar = await memberCmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            isMember = scalar is not null;
        }

        return new MessageAccess(
            senderUserId,
            receiverUserId,
            conversationId,
            recalledAtMs,
            isMember);
    }

    private async Task<bool> ReactionExistsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long userId,
        string emoji,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = @message_id
               AND user_id = @user_id
               AND emoji = @emoji
             LIMIT 1;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("emoji", emoji);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    private async Task<bool> EmojiExistsOnMessageAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        string emoji,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT 1
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = @message_id
               AND emoji = @emoji
             LIMIT 1;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("emoji", emoji);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is not null;
    }

    private async Task<int> CountUserReactionsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long userId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = @message_id
               AND user_id = @user_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("user_id", userId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int count ? count : Convert.ToInt32(result);
    }

    private async Task<int> CountDistinctEmojisAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(DISTINCT emoji)::int
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = @message_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int count ? count : Convert.ToInt32(result);
    }

    private async Task<int> CountEmojiAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        string emoji,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int
             FROM {_databaseSchema.MessageReactionsTableSql}
             WHERE message_id = @message_id
               AND emoji = @emoji;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("emoji", emoji);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is int count ? count : Convert.ToInt32(result);
    }

    private async Task BumpChangedAtAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        string messageId,
        long occurredAtMs,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.MessagesTableSql}
             SET changed_at_ms = GREATEST(changed_at_ms, @changed_at_ms)
             WHERE message_id = @message_id;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("message_id", messageId);
        command.Parameters.AddWithValue("changed_at_ms", occurredAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task InsertReactionEventsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        bool added,
        string messageId,
        string? conversationId,
        long reactorUserId,
        string reactorSessionId,
        long messageSenderUserId,
        long messageReceiverUserId,
        string emoji,
        int emojiCount,
        long occurredAtMs,
        CancellationToken ct)
    {
        string payloadJson;
        RealtimeEventType eventType;
        if (added)
        {
            eventType = RealtimeEventType.ReactionAdded;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionAddedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionAddedPayload);
        }
        else
        {
            eventType = RealtimeEventType.ReactionRemoved;
            payloadJson = JsonSerializer.Serialize(
                new RealtimeReactionRemovedPayload
                {
                    MessageId = messageId,
                    ConversationId = conversationId,
                    ReactorUserId = reactorUserId,
                    MessageSenderUserId = messageSenderUserId,
                    MessageReceiverUserId = messageReceiverUserId,
                    Emoji = emoji,
                    EmojiCount = emojiCount,
                    OccurredAtMs = occurredAtMs
                },
                RealtimeJsonSerializerContext.Default.RealtimeReactionRemovedPayload);
        }

        var traceParent = RealtimeTraceContext.CaptureTraceParent();
        var traceState = RealtimeTraceContext.CaptureTraceState();
        var targets = new HashSet<long> { messageSenderUserId, messageReceiverUserId };
        if (!string.IsNullOrWhiteSpace(conversationId)
            && ConversationId.IsGroup(conversationId))
        {
            var memberIds = await ConversationWriteCommands.ListActiveMemberUserIdsAsync(
                    connection,
                    transaction,
                    _databaseSchema,
                    conversationId,
                    ct)
                .ConfigureAwait(false);
            targets.Clear();
            foreach (var id in memberIds)
                targets.Add(id);
        }

        var events = new List<RealtimeEvent>(targets.Count);
        foreach (var targetUserId in targets)
        {
            var eventId = added
                ? MessageEventIdFactory.CreateReactionAddedEventId(
                    messageId,
                    targetUserId,
                    reactorUserId,
                    emoji,
                    occurredAtMs)
                : MessageEventIdFactory.CreateReactionRemovedEventId(
                    messageId,
                    targetUserId,
                    reactorUserId,
                    emoji,
                    occurredAtMs);

            events.Add(new RealtimeEvent
            {
                EventId = eventId,
                Type = eventType,
                TargetUserId = targetUserId,
                ActorUserId = reactorUserId,
                MessageId = messageId,
                SessionId = reactorSessionId,
                PayloadJson = payloadJson,
                OccurredAtMs = occurredAtMs,
                TraceParent = traceParent,
                TraceState = traceState
            });
        }

        await InsertOutboxManyAsync(connection, transaction, events, ct).ConfigureAwait(false);
    }

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
             INSERT INTO {_databaseSchema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES
                 {string.Join(",\n                 ", values)}
             ON CONFLICT (event_id) DO NOTHING;
             """;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private sealed record MessageAccess(
        long SenderUserId,
        long ReceiverUserId,
        string? ConversationId,
        long? RecalledAtMs,
        bool IsAllowed);
}

using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly ILogger<NpgsqlRealtimeMessageStore> _logger;

    public NpgsqlRealtimeMessageStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        ILogger<NpgsqlRealtimeMessageStore> logger)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _logger = logger;
    }

    public async Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_databaseSchema.MessagesTableSql} (
                message_id,
                client_message_id,
                sender_user_id,
                sender_session_id,
                receiver_user_id,
                content,
                received_at_ms,
                created_at_ms
            )
            VALUES (
                @message_id,
                @client_message_id,
                @sender_user_id,
                @sender_session_id,
                @receiver_user_id,
                @content,
                @received_at_ms,
                @created_at_ms
            )
            ON CONFLICT (sender_user_id, client_message_id) DO NOTHING;
            """,
            connection,
            transaction);

        command.Parameters.AddWithValue("message_id", message.MessageId);
        command.Parameters.AddWithValue("client_message_id", message.ClientMessageId);
        command.Parameters.AddWithValue("sender_user_id", message.SenderUserId);
        command.Parameters.AddWithValue("sender_session_id", message.SenderSessionId);
        command.Parameters.AddWithValue("receiver_user_id", message.ReceiverUserId);
        command.Parameters.AddWithValue("content", message.Content);
        command.Parameters.AddWithValue("received_at_ms", message.ReceivedAtMs);
        command.Parameters.AddWithValue("created_at_ms", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var affectedRows = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        var persistedMessageId = affectedRows > 0
            ? message.MessageId
            : await GetExistingMessageIdAsync(connection, transaction, message, ct).ConfigureAwait(false);

        var persistedEvent = CopyWithMessageId(eventToPublish, persistedMessageId);
        await InsertOutboxAsync(connection, transaction, persistedEvent, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        if (affectedRows > 0)
        {
            _logger.LogInformation(
                "实时消息已通过 Npgsql 写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                message.MessageId,
                message.SenderUserId,
                message.ReceiverUserId);
            return new RealtimeMessagePersistResult(true, persistedMessageId);
        }

        _logger.LogInformation(
            "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
            message.ClientMessageId,
            message.SenderUserId);

        return new RealtimeMessagePersistResult(false, persistedMessageId);
    }

    public async Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        RealtimeEvent eventToPublish,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(ct)
            .ConfigureAwait(false);

        long senderUserId;
        long receiverUserId;
        long? deliveredAtMs;
        long? readAtMs;

        await using (var command = new NpgsqlCommand(
                         $"SELECT sender_user_id, receiver_user_id, delivered_at_ms, read_at_ms FROM {_databaseSchema.MessagesTableSql} WHERE message_id = @message_id FOR UPDATE",
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            await using var reader = await command
                .ExecuteReaderAsync(ct)
                .ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.MessageNotFound,
                    receipt.MessageId);
            }

            senderUserId = reader.GetInt64(0);
            receiverUserId = reader.GetInt64(1);
            deliveredAtMs = reader.IsDBNull(2) ? null : reader.GetInt64(2);
            readAtMs = reader.IsDBNull(3) ? null : reader.GetInt64(3);
        }

        if (receiverUserId != receipt.ReceiverUserId)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.ReceiverMismatch,
                receipt.MessageId,
                senderUserId);
        }

        var shouldApply = receipt.ReceiptType switch
        {
            MessageReceiptType.Delivered => deliveredAtMs is null && readAtMs is null,
            MessageReceiptType.Read => readAtMs is null,
            _ => false
        };
        if (!shouldApply)
        {
            return new MessageReceiptPersistResult(
                MessageReceiptPersistStatus.Unchanged,
                receipt.MessageId,
                senderUserId);
        }

        var setClause = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms = @occurred_at_ms, delivered_at_ms = COALESCE(delivered_at_ms, @occurred_at_ms)"
            : "delivered_at_ms = @occurred_at_ms";
        var condition = receipt.ReceiptType == MessageReceiptType.Read
            ? "read_at_ms IS NULL"
            : "delivered_at_ms IS NULL AND read_at_ms IS NULL";

        await using (var command = new NpgsqlCommand(
                         $"UPDATE {_databaseSchema.MessagesTableSql} SET {setClause} WHERE message_id = @message_id AND receiver_user_id = @receiver_user_id AND {condition}",
                         connection,
                         transaction))
        {
            command.Parameters.AddWithValue("message_id", receipt.MessageId);
            command.Parameters.AddWithValue("receiver_user_id", receipt.ReceiverUserId);
            command.Parameters.AddWithValue("occurred_at_ms", receipt.OccurredAtMs);
            var affectedRows = await command
                .ExecuteNonQueryAsync(ct)
                .ConfigureAwait(false);
            if (affectedRows == 0)
            {
                return new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId,
                    senderUserId);
            }
        }

        await InsertOutboxAsync(
                connection,
                transaction,
                CopyForReceipt(eventToPublish, senderUserId),
                ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        return new MessageReceiptPersistResult(
            MessageReceiptPersistStatus.Applied,
            receipt.MessageId,
            senderUserId);
    }
    private async Task<string> GetExistingMessageIdAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeMessageRecord message,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT message_id FROM {_databaseSchema.MessagesTableSql} WHERE sender_user_id = @sender_user_id AND client_message_id = @client_message_id",
            connection,
            transaction);
        command.Parameters.AddWithValue("sender_user_id", message.SenderUserId);
        command.Parameters.AddWithValue("client_message_id", message.ClientMessageId);
        var existing = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return existing as string
               ?? throw new InvalidOperationException("检测到消息冲突，但无法读取已有消息编号。");
    }

    private async Task InsertOutboxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type,
                 created_at_ms, next_attempt_at_ms, attempt_count
             ) VALUES (
                 @event_id, @payload_json, @target_user_id, @event_type,
                 @created_at_ms, @next_attempt_at_ms, 0
             )
             ON CONFLICT (event_id) DO NOTHING;
             """,
            connection,
            transaction);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        command.Parameters.AddWithValue("event_id", evt.EventId);
        command.Parameters.AddWithValue(
            "payload_json",
            JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
        command.Parameters.AddWithValue("target_user_id", evt.TargetUserId);
        command.Parameters.AddWithValue("event_type", (short)evt.Type);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static RealtimeEvent CopyForReceipt(
        RealtimeEvent evt,
        long senderUserId) => new()
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = senderUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            OccurredAtMs = evt.OccurredAtMs,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState
        };
    private static RealtimeEvent CopyWithMessageId(RealtimeEvent evt, string messageId) => new()
    {
        EventId = evt.EventId,
        Type = evt.Type,
        TargetUserId = evt.TargetUserId,
        ActorUserId = evt.ActorUserId,
        MessageId = messageId,
        SessionId = evt.SessionId,
        PayloadJson = evt.PayloadJson,
        OccurredAtMs = evt.OccurredAtMs,
        TraceParent = evt.TraceParent,
        TraceState = evt.TraceState
    };
    public async Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 5_000);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        long total = 0;
        while (true)
        {
            await using var command = new NpgsqlCommand(
                $"""
                 DELETE FROM {_databaseSchema.MessagesTableSql}
                 WHERE ctid IN (
                     SELECT ctid FROM {_databaseSchema.MessagesTableSql}
                     WHERE sender_user_id = @user_id OR receiver_user_id = @user_id
                     LIMIT @batch_size
                 );
                 """,
                connection);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("batch_size", batchSize);
            var deleted = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (deleted <= 0)
                break;
            total += deleted;
        }

        await using (var outboxCmd = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE target_user_id = @user_id
               AND event_type <> @keep_type;
             """,
            connection))
        {
            outboxCmd.Parameters.AddWithValue("user_id", userId);
            // AccountCleanupCompleted = 9；保留完成回传，避免重试抹掉待发布 Outbox。
            outboxCmd.Parameters.AddWithValue(
                "keep_type",
                (short)RealtimeEventType.AccountCleanupCompleted);
            var outboxDeleted = await outboxCmd.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
            if (total > 0 || outboxDeleted > 0)
            {
                _logger.LogInformation(
                    "已清理用户消息与 Outbox。用户={UserId}；删除消息={Deleted}；删除Outbox={OutboxDeleted}",
                    userId,
                    total,
                    outboxDeleted);
            }
        }

        return total;
    }

    public async Task EnqueueEventAsync(RealtimeEvent eventToPublish, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventToPublish);
        ArgumentException.ThrowIfNullOrWhiteSpace(eventToPublish.EventId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        await InsertOutboxAsync(connection, transaction, eventToPublish, ct).ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);
    }
}

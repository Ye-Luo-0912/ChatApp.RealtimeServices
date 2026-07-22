using System.Data;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeMessageHistoryStore : IRealtimeMessageHistoryStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeMessageHistoryStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<IReadOnlyList<RealtimeHistoryMessage>> QueryAsync(
        long userId,
        long? beforeReceivedAtMs,
        string? beforeMessageId,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        if (take is <= 0 or > 101)
            throw new ArgumentOutOfRangeException(nameof(take));

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 history.message_id,
                 history.client_message_id,
                 history.sender_user_id,
                 history.receiver_user_id,
                 history.content,
                 history.received_at_ms,
                 history.delivered_at_ms,
                 history.read_at_ms
             FROM (
                 (
                     SELECT
                         message_id,
                         client_message_id,
                         sender_user_id,
                         receiver_user_id,
                         content,
                         received_at_ms,
                         delivered_at_ms,
                         read_at_ms
                     FROM {_databaseSchema.MessagesTableSql}
                     WHERE receiver_user_id = @user_id
                       AND (
                           @before_received_at_ms IS NULL
                           OR received_at_ms < @before_received_at_ms
                           OR (
                               received_at_ms = @before_received_at_ms
                               AND message_id < @before_message_id
                           )
                       )
                     ORDER BY received_at_ms DESC, message_id DESC
                     LIMIT @take
                 )
                 UNION ALL
                 (
                     SELECT
                         message_id,
                         client_message_id,
                         sender_user_id,
                         receiver_user_id,
                         content,
                         received_at_ms,
                         delivered_at_ms,
                         read_at_ms
                     FROM {_databaseSchema.MessagesTableSql}
                     WHERE sender_user_id = @user_id
                       AND receiver_user_id <> @user_id
                       AND (
                           @before_received_at_ms IS NULL
                           OR received_at_ms < @before_received_at_ms
                           OR (
                               received_at_ms = @before_received_at_ms
                               AND message_id < @before_message_id
                           )
                       )
                     ORDER BY received_at_ms DESC, message_id DESC
                     LIMIT @take
                 )
             ) AS history
             ORDER BY history.received_at_ms DESC, history.message_id DESC
             LIMIT @take;
             """,
            connection);

        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("take", take);
        command.Parameters
            .Add("before_received_at_ms", NpgsqlDbType.Bigint)
            .Value = beforeReceivedAtMs.HasValue
                ? beforeReceivedAtMs.Value
                : DBNull.Value;
        command.Parameters
            .Add("before_message_id", NpgsqlDbType.Varchar)
            .Value = beforeMessageId is not null
                ? beforeMessageId
                : DBNull.Value;

        var messages = new List<RealtimeHistoryMessage>(take);
        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SequentialAccess, ct)
            .ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            messages.Add(new RealtimeHistoryMessage
            {
                MessageId = reader.GetString(0),
                ClientMessageId = reader.GetString(1),
                SenderUserId = reader.GetInt64(2),
                ReceiverUserId = reader.GetInt64(3),
                Content = reader.GetString(4),
                ReceivedAtMs = reader.GetInt64(5),
                DeliveredAtMs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
                ReadAtMs = reader.IsDBNull(7) ? null : reader.GetInt64(7)
            });
        }

        return messages;
    }

    public async Task<RealtimeHistoryMessage?> TryGetByIdAsync(
        string messageId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(messageId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 message_id,
                 client_message_id,
                 sender_user_id,
                 receiver_user_id,
                 content,
                 received_at_ms,
                 delivered_at_ms,
                 read_at_ms
             FROM {_databaseSchema.MessagesTableSql}
             WHERE message_id = @message_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("message_id", messageId.Trim());

        await using var reader = await command
            .ExecuteReaderAsync(CommandBehavior.SingleRow, ct)
            .ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new RealtimeHistoryMessage
        {
            MessageId = reader.GetString(0),
            ClientMessageId = reader.GetString(1),
            SenderUserId = reader.GetInt64(2),
            ReceiverUserId = reader.GetInt64(3),
            Content = reader.GetString(4),
            ReceivedAtMs = reader.GetInt64(5),
            DeliveredAtMs = reader.IsDBNull(6) ? null : reader.GetInt64(6),
            ReadAtMs = reader.IsDBNull(7) ? null : reader.GetInt64(7)
        };
    }
}
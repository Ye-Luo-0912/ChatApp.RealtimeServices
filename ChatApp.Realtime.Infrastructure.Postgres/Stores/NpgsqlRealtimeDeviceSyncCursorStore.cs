using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeDeviceSyncCursorStore : IRealtimeDeviceSyncCursorStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeDeviceSyncCursorStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<IReadOnlyList<DeviceSyncCursor>> LoadAsync(
        long userId,
        ulong deviceIdHash,
        int take,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        take = Math.Clamp(take, 1, 50);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT conversation_id, after_received_at_ms, after_message_id
             FROM {_databaseSchema.DeviceSyncCursorsTableSql}
             WHERE user_id = @user_id
               AND device_id_hash = @device_id_hash
             ORDER BY updated_at_ms DESC
             LIMIT @take;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("device_id_hash", unchecked((long)deviceIdHash));
        command.Parameters.AddWithValue("take", take);

        var items = new List<DeviceSyncCursor>(take);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new DeviceSyncCursor
            {
                ConversationId = reader.GetString(0),
                AfterReceivedAtMs = reader.GetInt64(1),
                AfterMessageId = reader.GetString(2)
            });
        }

        return items;
    }

    public async Task UpsertManyAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<DeviceSyncCursor> cursors,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        if (cursors.Count == 0)
            return;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var deviceHash = unchecked((long)deviceIdHash);

        foreach (var cursor in cursors)
        {
            if (string.IsNullOrWhiteSpace(cursor.ConversationId)
                || string.IsNullOrWhiteSpace(cursor.AfterMessageId)
                || cursor.AfterReceivedAtMs <= 0)
            {
                continue;
            }

            await using var command = new NpgsqlCommand(
                $"""
                 INSERT INTO {_databaseSchema.DeviceSyncCursorsTableSql} (
                     user_id, device_id_hash, conversation_id,
                     after_received_at_ms, after_message_id, updated_at_ms
                 ) VALUES (
                     @user_id, @device_id_hash, @conversation_id,
                     @after_received_at_ms, @after_message_id, @updated_at_ms
                 )
                 ON CONFLICT (user_id, device_id_hash, conversation_id) DO UPDATE SET
                     after_received_at_ms = EXCLUDED.after_received_at_ms,
                     after_message_id = EXCLUDED.after_message_id,
                     updated_at_ms = EXCLUDED.updated_at_ms
                 WHERE ({_databaseSchema.DeviceSyncCursorsTableSql}.after_received_at_ms,
                        {_databaseSchema.DeviceSyncCursorsTableSql}.after_message_id)
                       < (EXCLUDED.after_received_at_ms, EXCLUDED.after_message_id);
                 """,
                connection,
                transaction);
            command.Parameters.AddWithValue("user_id", userId);
            command.Parameters.AddWithValue("device_id_hash", deviceHash);
            command.Parameters.AddWithValue("conversation_id", cursor.ConversationId.Trim());
            command.Parameters.AddWithValue("after_received_at_ms", cursor.AfterReceivedAtMs);
            command.Parameters.AddWithValue("after_message_id", cursor.AfterMessageId.Trim());
            command.Parameters.AddWithValue("updated_at_ms", now);
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
    }

    public async Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.DeviceSyncCursorsTableSql}
             WHERE user_id = @user_id;
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

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

        // Perf-3：过滤无效项后用单条 UNNEST 批量 UPSERT，避免逐游标数据库往返。
        var conversationIds = new List<string>(cursors.Count);
        var afterReceivedAtMs = new List<long>(cursors.Count);
        var afterMessageIds = new List<string>(cursors.Count);
        foreach (var cursor in cursors)
        {
            if (string.IsNullOrWhiteSpace(cursor.ConversationId)
                || string.IsNullOrWhiteSpace(cursor.AfterMessageId)
                || cursor.AfterReceivedAtMs <= 0)
            {
                continue;
            }
            conversationIds.Add(cursor.ConversationId.Trim());
            afterReceivedAtMs.Add(cursor.AfterReceivedAtMs);
            afterMessageIds.Add(cursor.AfterMessageId.Trim());
        }

        if (conversationIds.Count == 0)
            return;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var deviceHash = unchecked((long)deviceIdHash);

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        // 固定命令文本（不随行数变化），避免污染 Npgsql AutoPrepare 缓存。
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.DeviceSyncCursorsTableSql} (
                 user_id, device_id_hash, conversation_id,
                 after_received_at_ms, after_message_id, updated_at_ms, last_seen_at_ms
             )
             SELECT @user_id, @device_id_hash, arr.conversation_id,
                    arr.after_received_at_ms, arr.after_message_id, @now, @now
             FROM UNNEST(@conversation_ids, @after_received_at_ms_arr, @after_message_ids)
                 AS arr(conversation_id, after_received_at_ms, after_message_id)
             ON CONFLICT (user_id, device_id_hash, conversation_id) DO UPDATE SET
                 after_received_at_ms = EXCLUDED.after_received_at_ms,
                 after_message_id = EXCLUDED.after_message_id,
                 updated_at_ms = EXCLUDED.updated_at_ms,
                 last_seen_at_ms = EXCLUDED.last_seen_at_ms
             WHERE ({_databaseSchema.DeviceSyncCursorsTableSql}.after_received_at_ms,
                    {_databaseSchema.DeviceSyncCursorsTableSql}.after_message_id)
                   < (EXCLUDED.after_received_at_ms, EXCLUDED.after_message_id);
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("device_id_hash", deviceHash);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("conversation_ids", conversationIds.ToArray());
        command.Parameters.AddWithValue("after_received_at_ms_arr", afterReceivedAtMs.ToArray());
        command.Parameters.AddWithValue("after_message_ids", afterMessageIds.ToArray());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task DeleteAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<string> conversationIds,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(userId);
        if (conversationIds.Count == 0)
            return;

        var distinctIds = conversationIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (distinctIds.Length == 0)
            return;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.DeviceSyncCursorsTableSql}
             WHERE user_id = @user_id
               AND device_id_hash = @device_id_hash
               AND conversation_id = ANY(@conversation_ids);
             """,
            connection);
        command.Parameters.AddWithValue("user_id", userId);
        command.Parameters.AddWithValue("device_id_hash", unchecked((long)deviceIdHash));
        var idsParam = command.Parameters.Add(
            "conversation_ids",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        idsParam.Value = distinctIds;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

    public async Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default)
    {
        // Perf-3：按 last_seen_at_ms 分批清理长期未活跃设备游标，避免单次大锁。
        batchSize = Math.Clamp(batchSize, 1, 10_000);
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.DeviceSyncCursorsTableSql}
             WHERE ctid IN (
                 SELECT ctid
                 FROM {_databaseSchema.DeviceSyncCursorsTableSql}
                 WHERE last_seen_at_ms < @inactive_before_ms
                 ORDER BY last_seen_at_ms
                 LIMIT @batch_size
             );
             """,
            connection);
        command.Parameters.AddWithValue("inactive_before_ms", inactiveBeforeMs);
        command.Parameters.AddWithValue("batch_size", batchSize);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

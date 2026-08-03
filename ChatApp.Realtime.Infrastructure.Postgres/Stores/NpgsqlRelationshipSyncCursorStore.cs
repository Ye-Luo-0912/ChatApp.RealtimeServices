using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// PostgreSQL 关系列表同步游标实现。
/// <para>
/// 与 <see cref="NpgsqlRealtimeDeviceSyncCursorStore"/> 平行，但以 list_type 为维度。
/// Upsert 使用单调推进 WHERE 子句：仅当新水位 > 已存水位时更新。
/// </para>
/// </summary>
public sealed class NpgsqlRelationshipSyncCursorStore : IRelationshipSyncCursorStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _schema;

    public NpgsqlRelationshipSyncCursorStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema)
    {
        _databaseClient = databaseClient;
        _schema = schema;
    }

    public async Task<IReadOnlyList<RelationshipSyncCursor>> LoadAsync(
        long userId,
        ulong deviceIdHash,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"SELECT \"list_type\", \"after_changed_at_ms\", \"updated_at_ms\", \"last_seen_at_ms\" " +
            $"FROM {_schema.RelationshipSyncCursorsTableSql} " +
            $"WHERE \"user_id\" = @uid AND \"device_id_hash\" = @did " +
            $"ORDER BY \"list_type\"",
            connection);
        command.Parameters.AddWithValue("uid", userId);
        command.Parameters.AddWithValue("did", (long)deviceIdHash);

        var results = new List<RelationshipSyncCursor>(3);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            results.Add(new RelationshipSyncCursor
            {
                ListType = reader.GetByte(0),
                AfterChangedAtMs = reader.GetInt64(1),
                UpdatedAtMs = reader.GetInt64(2),
                LastSeenAtMs = reader.GetInt64(3),
            });
        }
        return results;
    }

    public async Task UpsertManyAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<RelationshipSyncCursor> cursors,
        CancellationToken ct = default)
    {
        if (cursors.Count == 0)
            return;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        foreach (var cursor in cursors)
        {
            await using var command = new NpgsqlCommand(
                $"""
                 INSERT INTO {_schema.RelationshipSyncCursorsTableSql}
                 ("user_id", "device_id_hash", "list_type", "after_changed_at_ms", "updated_at_ms", "last_seen_at_ms")
                 VALUES (@uid, @did, @lt, @ac, @upd, @ls)
                 ON CONFLICT ("user_id", "device_id_hash", "list_type")
                 DO UPDATE SET
                     "after_changed_at_ms" = EXCLUDED."after_changed_at_ms",
                     "updated_at_ms" = EXCLUDED."updated_at_ms",
                     "last_seen_at_ms" = EXCLUDED."last_seen_at_ms"
                 WHERE {_schema.RelationshipSyncCursorsTableSql}."after_changed_at_ms" < EXCLUDED."after_changed_at_ms"
                 """,
                connection);
            command.Parameters.AddWithValue("uid", userId);
            command.Parameters.AddWithValue("did", (long)deviceIdHash);
            command.Parameters.AddWithValue("lt", (short)cursor.ListType);
            command.Parameters.AddWithValue("ac", cursor.AfterChangedAtMs);
            command.Parameters.AddWithValue("upd", now);
            command.Parameters.AddWithValue("ls", now);
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public async Task DeleteAsync(
        long userId,
        ulong deviceIdHash,
        IReadOnlyList<byte> listTypes,
        CancellationToken ct = default)
    {
        if (listTypes.Count == 0)
            return;

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        foreach (var listType in listTypes)
        {
            await using var command = new NpgsqlCommand(
                $"DELETE FROM {_schema.RelationshipSyncCursorsTableSql} " +
                $"WHERE \"user_id\" = @uid AND \"device_id_hash\" = @did AND \"list_type\" = @lt",
                connection);
            command.Parameters.AddWithValue("uid", userId);
            command.Parameters.AddWithValue("did", (long)deviceIdHash);
            command.Parameters.AddWithValue("lt", (short)listType);
            await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }
    }

    public async Task<long> DeleteByUserAsync(long userId, CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"DELETE FROM {_schema.RelationshipSyncCursorsTableSql} WHERE \"user_id\" = @uid",
            connection);
        command.Parameters.AddWithValue("uid", userId);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<long> DeleteInactiveAsync(long inactiveBeforeMs, int batchSize, CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"DELETE FROM {_schema.RelationshipSyncCursorsTableSql} " +
            $"WHERE \"ctid\" IN (SELECT \"ctid\" FROM {_schema.RelationshipSyncCursorsTableSql} " +
            $"WHERE \"last_seen_at_ms\" < @before LIMIT @batch)",
            connection);
        command.Parameters.AddWithValue("before", inactiveBeforeMs);
        command.Parameters.AddWithValue("batch", batchSize);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}
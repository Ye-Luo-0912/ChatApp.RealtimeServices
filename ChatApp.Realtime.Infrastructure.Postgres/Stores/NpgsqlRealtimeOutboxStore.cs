using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeOutboxStore : IRealtimeOutboxStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;

    public NpgsqlRealtimeOutboxStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
    }

    public async Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
        string instanceId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lockedUntil = now + (long)leaseDuration.TotalMilliseconds;
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             WITH candidates AS (
                 SELECT event_id
                 FROM {_databaseSchema.OutboxTableSql}
                 WHERE status = {(short)RealtimeOutboxStatus.Pending}
                   AND published_at_ms IS NULL
                   AND next_attempt_at_ms <= @now
                   AND (locked_until_ms IS NULL OR locked_until_ms < @now)
                 ORDER BY created_at_ms
                 FOR UPDATE SKIP LOCKED
                 LIMIT @batch_size
             )
             UPDATE {_databaseSchema.OutboxTableSql} AS item
             SET locked_by = @instance_id,
                 locked_until_ms = @locked_until,
                 attempt_count = item.attempt_count + 1
             FROM candidates
             WHERE item.event_id = candidates.event_id
             RETURNING item.event_id, item.payload_json, item.attempt_count, item.locked_by;
             """,
            connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("batch_size", batchSize);
        command.Parameters.AddWithValue("instance_id", instanceId);
        command.Parameters.AddWithValue("locked_until", lockedUntil);

        var records = new List<RealtimeOutboxRecord>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            var evt = JsonSerializer.Deserialize(
                          reader.GetString(1),
                          RealtimeJsonSerializerContext.Default.RealtimeEvent)
                      ?? throw new JsonException("Outbox 事件反序列化结果为空。");
            records.Add(new RealtimeOutboxRecord(
                reader.GetString(0),
                evt,
                reader.GetInt32(2),
                reader.GetString(3)));
        }

        return records;
    }

    public Task MarkPublishedAsync(RealtimeOutboxRecord record, CancellationToken ct = default) =>
        UpdateAsync(
            record,
            $"""
             published_at_ms = @now,
             status = {(short)RealtimeOutboxStatus.Published},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = NULL
             """,
            null,
            ct);

    public Task MarkFailedAsync(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan retryDelay,
        CancellationToken ct = default) =>
        UpdateAsync(
            record,
            "next_attempt_at_ms = @next_attempt, locked_by = NULL, locked_until_ms = NULL, last_error = @error",
            (error.Length <= 2048 ? error : error[..2048], retryDelay),
            ct);

    public Task MarkDeadAsync(
        RealtimeOutboxRecord record,
        string error,
        CancellationToken ct = default) =>
        UpdateAsync(
            record,
            $"""
             status = {(short)RealtimeOutboxStatus.Dead},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = @error,
             next_attempt_at_ms = @now
             """,
            (error.Length <= 2048 ? error : error[..2048], TimeSpan.Zero),
            ct);

    public async Task<bool> ReplayDeadAsync(string eventId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventId);
        var replayed = await ReplayDeadBatchAsync([eventId.Trim()], ct).ConfigureAwait(false);
        return replayed.Count > 0;
    }

    public async Task<IReadOnlyList<string>> ReplayDeadBatchAsync(
        IReadOnlyList<string> eventIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventIds);
        if (eventIds.Count == 0)
            return [];

        var normalized = eventIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .Take(500)
            .ToArray();
        if (normalized.Length == 0)
            return [];

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.OutboxTableSql}
             SET status = {(short)RealtimeOutboxStatus.Pending},
                 published_at_ms = NULL,
                 attempt_count = 0,
                 next_attempt_at_ms = @now,
                 locked_by = NULL,
                 locked_until_ms = NULL,
                 last_error = NULL
             WHERE event_id = ANY(@event_ids)
               AND status = {(short)RealtimeOutboxStatus.Dead}
             RETURNING event_id;
             """,
            connection);
        command.Parameters.AddWithValue("now", now);
        var idsParam = command.Parameters.Add(
            "event_ids",
            NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text);
        idsParam.Value = normalized;

        var replayed = new List<string>(normalized.Length);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            replayed.Add(reader.GetString(0));

        return replayed;
    }

    public async Task<int> CleanupPublishedAsync(
        long publishedBeforeMs,
        int batchSize,
        CancellationToken ct = default)
    {
        batchSize = Math.Clamp(batchSize, 1, 10_000);
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE ctid IN (
                 SELECT ctid
                 FROM {_databaseSchema.OutboxTableSql}
                 WHERE status = {(short)RealtimeOutboxStatus.Published}
                   AND published_at_ms IS NOT NULL
                   AND published_at_ms < @cutoff
                 ORDER BY published_at_ms
                 LIMIT @batch_size
             );
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", publishedBeforeMs);
        command.Parameters.AddWithValue("batch_size", batchSize);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<RealtimeOutboxStats> GetStatsAsync(CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var pending = (short)RealtimeOutboxStatus.Pending;
        var dead = (short)RealtimeOutboxStatus.Dead;
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        // 仅扫 Pending / Dead（走部分索引）；子查询避免 Published 全表聚合。
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT COUNT(*)::bigint
                  FROM {_databaseSchema.OutboxTableSql}
                  WHERE status = {pending}),
                 (SELECT MIN(created_at_ms)
                  FROM {_databaseSchema.OutboxTableSql}
                  WHERE status = {pending}),
                 (SELECT COALESCE(MAX(attempt_count), 0)
                  FROM {_databaseSchema.OutboxTableSql}
                  WHERE status = {pending}),
                 (SELECT COUNT(*)::bigint
                  FROM {_databaseSchema.OutboxTableSql}
                  WHERE status = {dead}),
                 (SELECT MIN(created_at_ms)
                  FROM {_databaseSchema.OutboxTableSql}
                  WHERE status = {pending}
                    AND locked_until_ms IS NOT NULL
                    AND locked_until_ms >= @now);
             """,
            connection);
        command.Parameters.AddWithValue("now", now);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await reader.ReadAsync(ct).ConfigureAwait(false);
        return new RealtimeOutboxStats(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetInt64(1),
            reader.GetInt32(2),
            reader.GetInt64(3),
            reader.IsDBNull(4) ? null : reader.GetInt64(4));
    }

    public async Task<IReadOnlyList<RealtimeOutboxListItem>> ListAsync(
        RealtimeOutboxStatus? status,
        long? targetUserId,
        int offset,
        int limit,
        CancellationToken ct = default)
    {
        offset = Math.Max(0, offset);
        limit = Math.Clamp(limit, 1, 200);

        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 event_id,
                 status,
                 event_type,
                 target_user_id,
                 attempt_count,
                 created_at_ms,
                 next_attempt_at_ms,
                 published_at_ms,
                 locked_by,
                 locked_until_ms,
                 last_error
             FROM {_databaseSchema.OutboxTableSql}
             WHERE (@status IS NULL OR status = @status)
               AND (@target_user_id IS NULL OR target_user_id = @target_user_id)
             ORDER BY created_at_ms DESC
             OFFSET @offset
             LIMIT @limit;
             """,
            connection);
        command.Parameters.AddWithValue("offset", offset);
        command.Parameters.AddWithValue("limit", limit);
        var statusParam = command.Parameters.Add("status", NpgsqlTypes.NpgsqlDbType.Smallint);
        statusParam.Value = status.HasValue ? (short)status.Value : DBNull.Value;
        var userParam = command.Parameters.Add("target_user_id", NpgsqlTypes.NpgsqlDbType.Bigint);
        userParam.Value = targetUserId.HasValue ? targetUserId.Value : DBNull.Value;

        var items = new List<RealtimeOutboxListItem>(limit);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new RealtimeOutboxListItem(
                reader.GetString(0),
                (RealtimeOutboxStatus)reader.GetInt16(1),
                reader.GetInt16(2),
                reader.GetInt64(3),
                reader.GetInt32(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetString(8),
                reader.IsDBNull(9) ? null : reader.GetInt64(9),
                reader.IsDBNull(10) ? null : reader.GetString(10)));
        }

        return items;
    }

    public async Task<RealtimeOutboxListItem?> TryGetAsync(string eventId, CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventId);
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 event_id,
                 status,
                 event_type,
                 target_user_id,
                 attempt_count,
                 created_at_ms,
                 next_attempt_at_ms,
                 published_at_ms,
                 locked_by,
                 locked_until_ms,
                 last_error
             FROM {_databaseSchema.OutboxTableSql}
             WHERE event_id = @event_id
             LIMIT 1;
             """,
            connection);
        command.Parameters.AddWithValue("event_id", eventId.Trim());

        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;

        return new RealtimeOutboxListItem(
            reader.GetString(0),
            (RealtimeOutboxStatus)reader.GetInt16(1),
            reader.GetInt16(2),
            reader.GetInt64(3),
            reader.GetInt32(4),
            reader.GetInt64(5),
            reader.GetInt64(6),
            reader.IsDBNull(7) ? null : reader.GetInt64(7),
            reader.IsDBNull(8) ? null : reader.GetString(8),
            reader.IsDBNull(9) ? null : reader.GetInt64(9),
            reader.IsDBNull(10) ? null : reader.GetString(10));
    }

    private async Task UpdateAsync(
        RealtimeOutboxRecord record,
        string setClause,
        (string Error, TimeSpan Delay)? failure,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"UPDATE {_databaseSchema.OutboxTableSql} SET {setClause} WHERE event_id = @event_id AND locked_by = @lock_owner",
            connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("event_id", record.EventId);
        command.Parameters.AddWithValue("lock_owner", record.LockOwner);
        if (failure is not null)
        {
            command.Parameters.AddWithValue(
                "next_attempt",
                now + (long)failure.Value.Delay.TotalMilliseconds);
            command.Parameters.AddWithValue("error", failure.Value.Error);
        }

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

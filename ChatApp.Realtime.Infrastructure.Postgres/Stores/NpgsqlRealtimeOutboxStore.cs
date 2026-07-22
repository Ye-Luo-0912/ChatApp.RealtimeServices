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
                 WHERE published_at_ms IS NULL
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
            "published_at_ms = @now, locked_by = NULL, locked_until_ms = NULL, last_error = NULL",
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

    public async Task<RealtimeOutboxStats> GetStatsAsync(CancellationToken ct = default)
    {
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT COUNT(*), MIN(created_at_ms), COALESCE(MAX(attempt_count), 0)
             FROM {_databaseSchema.OutboxTableSql}
             WHERE published_at_ms IS NULL;
             """,
            connection);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        await reader.ReadAsync(ct).ConfigureAwait(false);
        return new RealtimeOutboxStats(
            reader.GetInt64(0),
            reader.IsDBNull(1) ? null : reader.GetInt64(1),
            reader.GetInt32(2));
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
            command.Parameters.AddWithValue("next_attempt", now + (long)failure.Value.Delay.TotalMilliseconds);
            command.Parameters.AddWithValue("error", failure.Value.Error);
        }

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

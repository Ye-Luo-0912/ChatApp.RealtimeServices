using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Outbox;

/// <summary>
/// Outbox 写入：将业务事件以 <c>ON CONFLICT (event_id) DO NOTHING</c> 的幂等方式
/// 写入当前事务的 Outbox 表，支持单条与批量（聚合事件携带 <c>TargetUserIds</c>）。
/// </summary>
internal sealed class PostgresOutboxWriter
{
    private readonly RealtimeWriteSession _session;

    public PostgresOutboxWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    public Task InsertAsync(RealtimeEvent evt) => InsertManyAsync([evt]);

    public async Task InsertManyAsync(IReadOnlyList<RealtimeEvent> events)
    {
        if (events.Count == 0)
            return;

        var ct = _session.CancellationToken;
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var command = new NpgsqlCommand
        {
            Connection = _session.Connection,
            Transaction = _session.Transaction
        };
        var values = new List<string>(events.Count);
        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            values.Add(
                $"(@event_id_{i}, @payload_json_{i}, @target_user_id_{i}, @event_type_{i}, @status, @created_at_ms, @next_attempt_at_ms, 0, @target_user_ids_{i})");
            command.Parameters.AddWithValue($"event_id_{i}", evt.EventId);
            command.Parameters.AddWithValue(
                $"payload_json_{i}",
                JsonSerializer.Serialize(evt, RealtimeJsonSerializerContext.Default.RealtimeEvent));
            command.Parameters.AddWithValue($"target_user_id_{i}", evt.TargetUserId);
            command.Parameters.AddWithValue($"event_type_{i}", (short)evt.Type);
            var targetUserIdsParam = command.Parameters.Add(
                $"target_user_ids_{i}",
                NpgsqlDbType.Array | NpgsqlDbType.Bigint);
            targetUserIdsParam.Value = (object?)evt.TargetUserIds ?? DBNull.Value;
        }

        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        command.CommandText =
            $"""
             INSERT INTO {_session.Schema.OutboxTableSql} (
                 event_id, payload_json, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids
             ) VALUES
                 {string.Join(",\n                 ", values)}
             ON CONFLICT (event_id) DO NOTHING;
             """;
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

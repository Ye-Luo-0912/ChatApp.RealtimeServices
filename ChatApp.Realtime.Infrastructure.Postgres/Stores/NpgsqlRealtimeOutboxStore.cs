using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
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
        // P1-3：每次 claim 生成不可复用的 lease token，避免同一实例标识在 lease 过期并
        // 重新领取后，旧任务误完成新 lease。
        var claimToken = Guid.NewGuid().ToString("N");
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
                 claim_token = @claim_token,
                 locked_until_ms = @locked_until,
                 attempt_count = item.attempt_count + 1
             FROM candidates
             WHERE item.event_id = candidates.event_id
             RETURNING item.event_id, item.event_type, item.target_user_id, item.target_user_ids,
                 item.audience_kind, item.conversation_id,
                 item.payload_json, item.payload_utf8, item.attempt_count, item.locked_by, item.claim_token;
             """,
            connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("batch_size", batchSize);
        command.Parameters.AddWithValue("instance_id", instanceId);
        command.Parameters.AddWithValue("claim_token", claimToken);
        command.Parameters.AddWithValue("locked_until", lockedUntil);

        var records = new List<RealtimeOutboxRecord>(batchSize);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            // 四-1/五：数据库列是投递目标的唯一权威，不再从 payload 反序列化路由信息。
            var eventId = reader.GetString(0);                                          // event_id
            var eventType = (RealtimeEventType)reader.GetInt16(1);                      // event_type
            var targetUserId = reader.GetInt64(2);                                      // target_user_id
            long[]? targetUserIds = null;
            if (!reader.IsDBNull(3))
                targetUserIds = reader.GetFieldValue<long[]>(3);                        // target_user_ids
            var audienceKindRaw = reader.IsDBNull(4) ? (short)0 : reader.GetInt16(4);   // audience_kind
            var conversationId = reader.IsDBNull(5) ? null : reader.GetString(5);       // conversation_id
            var payloadJson = reader.IsDBNull(6) ? null : reader.GetString(6);          // payload_json (新记录为 NULL)
            ReadOnlyMemory<byte>? payloadUtf8 = null;
            if (!reader.IsDBNull(7))
                payloadUtf8 = reader.GetFieldValue<byte[]>(7);                          // payload_utf8
            var attemptCount = reader.GetInt32(8);                                      // attempt_count
            var lockOwner = reader.GetString(9);                                        // locked_by
            var claimTokenFromRow = reader.GetString(10);                               // claim_token

            RealtimeEvent? evt = null;
            string? traceParent = null;
            string? traceState = null;

            if (payloadUtf8 is { Length: > 0 } utf8)
            {
                // 新记录：payload_utf8 已排除路由字段，用 JsonDocument 轻量提取 trace context。
                (traceParent, traceState) = ExtractTraceContext(utf8);
            }
            else if (payloadJson is not null)
            {
                // 旧记录：payload_utf8 为 NULL，反序列化 payload_json 获取路由与 trace 信息。
                evt = JsonSerializer.Deserialize(
                          payloadJson,
                          RealtimeJsonSerializerContext.Default.RealtimeEvent);
                if (evt is not null)
                {
                    // 旧记录的列可能为空，从反序列化的 event 补充路由信息。
                    if (targetUserIds is null && evt.TargetUserIds is { Length: > 0 })
                        targetUserIds = evt.TargetUserIds;
                    if (audienceKindRaw == 0 && evt.AudienceKind is not null)
                        audienceKindRaw = (short)evt.AudienceKind.Value;
                    traceParent = evt.TraceParent;
                    traceState = evt.TraceState;
                    // 旧记录无 payload_utf8，从 event 序列化生成 wire payload（排除路由字段）。
                    payloadUtf8 = JsonSerializer.SerializeToUtf8Bytes(
                        CreateWirePayload(evt),
                        RealtimeJsonSerializerContext.Default.RealtimeEvent);
                }
            }

            records.Add(new RealtimeOutboxRecord(
                eventId,
                eventType,
                targetUserId,
                targetUserIds,
                audienceKindRaw == 0 ? null : (AudienceKind)audienceKindRaw,
                conversationId,
                traceParent,
                traceState,
                evt,
                attemptCount,
                lockOwner,
                claimTokenFromRow,
                payloadUtf8));
        }

        return records;
    }

    /// <summary>
    /// 四-1/五：从 wire payload（已排除路由字段）轻量提取 W3C trace context。
    /// 使用 <see cref="JsonDocument"/> 做低分配 DOM 读取，不做完整反序列化。
    /// </summary>
    private static (string? TraceParent, string? TraceState) ExtractTraceContext(ReadOnlyMemory<byte> utf8Json)
    {
        try
        {
            using var doc = JsonDocument.Parse(utf8Json);
            if (!doc.RootElement.TryGetProperty("TraceParent", out var tp)
                || tp.ValueKind != JsonValueKind.String)
            {
                return (null, null);
            }

            var traceParent = tp.GetString();
            string? traceState = null;
            if (doc.RootElement.TryGetProperty("TraceState", out var ts)
                && ts.ValueKind == JsonValueKind.String)
            {
                traceState = ts.GetString();
            }

            return (traceParent, traceState);
        }
        catch (JsonException)
        {
            return (null, null);
        }
    }

    /// <summary>
    /// 四-1：创建 wire payload 副本，排除 <see cref="RealtimeEvent.TargetUserIds"/>
    /// 与 <see cref="RealtimeEvent.AudienceKind"/>。用于旧记录回退时生成 payload_utf8。
    /// </summary>
    private static RealtimeEvent CreateWirePayload(RealtimeEvent evt)
    {
        return new RealtimeEvent
        {
            EventId = evt.EventId,
            Type = evt.Type,
            TargetUserId = evt.TargetUserId,
            ActorUserId = evt.ActorUserId,
            MessageId = evt.MessageId,
            SessionId = evt.SessionId,
            PayloadJson = evt.PayloadJson,
            TraceParent = evt.TraceParent,
            TraceState = evt.TraceState,
            OccurredAtMs = evt.OccurredAtMs,
            ConversationId = evt.ConversationId,
            Payload = evt.Payload,
        };
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

    /// <summary>
    /// Perf-8：列出早于 cutoff 的 Dead 行，按 created_at_ms 升序、LIMIT 限定。
    /// 用于归档接收器落盘后再调用 <see cref="DeleteDeadBatchAsync"/> 物理删除。
    /// </summary>
    public async Task<IReadOnlyList<DeadOutboxRow>> ListDeadAsync(
        long createdBeforeMs,
        int limit,
        CancellationToken ct = default)
    {
        limit = Math.Clamp(limit, 1, 10_000);
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 event_id,
                 event_type,
                 target_user_id,
                 target_user_ids,
                 attempt_count,
                 created_at_ms,
                 next_attempt_at_ms,
                 last_error,
                 payload_json
             FROM {_databaseSchema.OutboxTableSql}
             WHERE status = {(short)RealtimeOutboxStatus.Dead}
               AND created_at_ms < @cutoff
             ORDER BY created_at_ms
             LIMIT @limit;
             """,
            connection);
        command.Parameters.AddWithValue("cutoff", createdBeforeMs);
        command.Parameters.AddWithValue("limit", limit);

        var rows = new List<DeadOutboxRow>(limit);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            rows.Add(new DeadOutboxRow(
                reader.GetString(0),
                reader.GetInt16(1),
                reader.GetInt64(2),
                reader.IsDBNull(3) ? null : reader.GetFieldValue<long[]>(3),
                reader.GetInt32(4),
                reader.GetInt64(5),
                reader.GetInt64(6),
                reader.IsDBNull(7) ? null : reader.GetString(7),
                reader.GetString(8)));
        }

        return rows;
    }

    /// <summary>
    /// Perf-8：按 event_id 批量删除 Dead 行。仅当归档成功（或选择跳过归档）后调用。
    /// 使用 <c>ctid IN</c> 子查询限定批次大小，避免全表锁。
    /// </summary>
    public async Task<int> DeleteDeadBatchAsync(
        IReadOnlyList<string> eventIds,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(eventIds);
        if (eventIds.Count == 0)
            return 0;

        var normalized = eventIds
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Select(static id => id.Trim())
            .Distinct(StringComparer.Ordinal)
            .Take(10_000)
            .ToArray();
        if (normalized.Length == 0)
            return 0;

        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE event_id = ANY(@event_ids)
               AND status = {(short)RealtimeOutboxStatus.Dead};
             """,
            connection);
        var idsParam = command.Parameters.Add(
            "event_ids",
            NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Text);
        idsParam.Value = normalized;
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
                 target_user_ids,
                 attempt_count,
                 created_at_ms,
                 next_attempt_at_ms,
                 published_at_ms,
                 locked_by,
                 locked_until_ms,
                 last_error
             FROM {_databaseSchema.OutboxTableSql}
             WHERE (@status IS NULL OR status = @status)
               AND (@target_user_id IS NULL OR target_user_id = @target_user_id OR @target_user_id = ANY(target_user_ids))
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
                reader.IsDBNull(4) ? null : reader.GetFieldValue<long[]>(4),
                reader.GetInt32(5),
                reader.GetInt64(6),
                reader.GetInt64(7),
                reader.IsDBNull(8) ? null : reader.GetInt64(8),
                reader.IsDBNull(9) ? null : reader.GetString(9),
                reader.IsDBNull(10) ? null : reader.GetInt64(10),
                reader.IsDBNull(11) ? null : reader.GetString(11)));
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
                 target_user_ids,
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
            reader.IsDBNull(4) ? null : reader.GetFieldValue<long[]>(4),
            reader.GetInt32(5),
            reader.GetInt64(6),
            reader.GetInt64(7),
            reader.IsDBNull(8) ? null : reader.GetInt64(8),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetInt64(10),
            reader.IsDBNull(11) ? null : reader.GetString(11));
    }

    public Task<int> MarkPublishedBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
            return Task.FromResult(0);

        var eventIds = records.Select(r => r.EventId).ToArray();
        var claimTokens = records.Select(r => r.ClaimToken).ToArray();
        return ExecuteBatchUpdateAsync(
            $"""
             published_at_ms = @now,
             status = {(short)RealtimeOutboxStatus.Published},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = NULL,
             claim_token = NULL
             """,
            eventIds,
            claimTokens,
            nextAttempts: null,
            errors: null,
            ct);
    }

    public Task<int> MarkFailedBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error, TimeSpan RetryDelay)> failures,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(failures);
        if (failures.Count == 0)
            return Task.FromResult(0);

        var eventIds = failures.Select(f => f.Record.EventId).ToArray();
        var claimTokens = failures.Select(f => f.Record.ClaimToken).ToArray();
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var nextAttempts = failures
            .Select(f => now + (long)f.RetryDelay.TotalMilliseconds)
            .ToArray();
        var errors = failures
            .Select(f => f.Error.Length <= 2048 ? f.Error : f.Error[..2048])
            .ToArray();
        return ExecuteBatchUpdateAsync(
            "next_attempt_at_ms = arr.next_attempt, locked_by = NULL, locked_until_ms = NULL, last_error = arr.error, claim_token = NULL",
            eventIds,
            claimTokens,
            nextAttempts,
            errors,
            ct);
    }

    public Task<int> MarkDeadBatchAsync(
        IReadOnlyList<(RealtimeOutboxRecord Record, string Error)> deadLetters,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(deadLetters);
        if (deadLetters.Count == 0)
            return Task.FromResult(0);

        var eventIds = deadLetters.Select(d => d.Record.EventId).ToArray();
        var claimTokens = deadLetters.Select(d => d.Record.ClaimToken).ToArray();
        var errors = deadLetters
            .Select(d => d.Error.Length <= 2048 ? d.Error : d.Error[..2048])
            .ToArray();
        return ExecuteBatchUpdateAsync(
            $"""
             status = {(short)RealtimeOutboxStatus.Dead},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = arr.error,
             next_attempt_at_ms = @now,
             claim_token = NULL
             """,
            eventIds,
            claimTokens,
            nextAttempts: null,
            errors,
            ct);
    }

    /// <summary>
    /// P1-3：批量续租已认领记录的 lease。用 UNNEST 配对 event_id + claim_token 校验所有权，
    /// 仅续租仍处于 Pending 且 claim_token 匹配的记录，防止续租已被其他实例认领的记录。
    /// 返回受影响行数。
    /// </summary>
    public async Task<int> ExtendLeaseBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        TimeSpan leaseExtension,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
            return 0;

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var lockedUntil = now + (long)leaseExtension.TotalMilliseconds;
        var eventIds = records.Select(r => r.EventId).ToArray();
        var claimTokens = records.Select(r => r.ClaimToken).ToArray();

        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.OutboxTableSql} AS item
             SET locked_until_ms = @locked_until
             FROM UNNEST(@event_ids, @claim_tokens) AS arr(event_id, claim_token)
             WHERE item.event_id = arr.event_id
               AND item.claim_token = arr.claim_token
               AND item.status = {(short)RealtimeOutboxStatus.Pending}
             """,
            connection);
        command.Parameters.AddWithValue("locked_until", lockedUntil);
        command.Parameters.AddWithValue("event_ids", eventIds);
        command.Parameters.AddWithValue("claim_tokens", claimTokens);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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
        // P1-3：用 claim_token 替代 locked_by 做所有权校验，避免同一实例标识在 lease
        // 过期并重新领取后，旧任务误完成新 lease。
        await using var command = new NpgsqlCommand(
            $"UPDATE {_databaseSchema.OutboxTableSql} SET {setClause} WHERE event_id = @event_id AND claim_token = @claim_token",
            connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("event_id", record.EventId);
        command.Parameters.AddWithValue("claim_token", record.ClaimToken);
        if (failure is not null)
        {
            command.Parameters.AddWithValue(
                "next_attempt",
                now + (long)failure.Value.Delay.TotalMilliseconds);
            command.Parameters.AddWithValue("error", failure.Value.Error);
        }

        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// P1-3：批量状态更新。用 UNNEST 配对 event_id + claim_token 校验所有权，
    /// 单次 UPDATE 完成一批记录的状态变更，避免逐事件数据库往返。返回受影响行数。
    /// </summary>
    private async Task<int> ExecuteBatchUpdateAsync(
        string setClause,
        string[] eventIds,
        string[] claimTokens,
        long[]? nextAttempts,
        string[]? errors,
        CancellationToken ct)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);

        // 当 setClause 引用 arr.next_attempt / arr.error 时，UNNEST 必须提供这些列。
        var unnestColumns = "event_id, claim_token";
        var unnestArgs = "@event_ids, @claim_tokens";
        if (nextAttempts is not null)
        {
            unnestColumns += ", next_attempt";
            unnestArgs += ", @next_attempts";
        }
        if (errors is not null)
        {
            unnestColumns += ", error";
            unnestArgs += ", @errors";
        }

        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.OutboxTableSql} AS item
             SET {setClause}
             FROM UNNEST({unnestArgs}) AS arr({unnestColumns})
             WHERE item.event_id = arr.event_id
               AND item.claim_token = arr.claim_token
             """,
            connection);
        command.Parameters.AddWithValue("now", now);
        command.Parameters.AddWithValue("event_ids", eventIds);
        command.Parameters.AddWithValue("claim_tokens", claimTokens);
        if (nextAttempts is not null)
            command.Parameters.AddWithValue("next_attempts", nextAttempts);
        if (errors is not null)
            command.Parameters.AddWithValue("errors", errors);

        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }
}

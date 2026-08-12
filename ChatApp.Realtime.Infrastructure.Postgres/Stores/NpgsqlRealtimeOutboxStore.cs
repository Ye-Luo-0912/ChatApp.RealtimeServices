using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeOutboxStore :
    IRealtimeOutboxStore,
    IRealtimeOutboxHintClaimStore,
    IRealtimeOutboxPreclaimedStore,
    IRealtimeOutboxClaimSessionFactory,
    IRealtimeOutboxCompactionStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly RealtimeMetrics? _metrics;

    public NpgsqlRealtimeOutboxStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        RealtimeMetrics? metrics = null)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _metrics = metrics;
    }

    public async ValueTask<IRealtimeOutboxClaimSession> OpenClaimSessionAsync(
        string instanceId,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        try
        {
            var session = new ClaimSession(this, connection, instanceId);
            await session.PrepareAsync(ct).ConfigureAwait(false);
            return session;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    public async Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
        string instanceId,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = CreateClaimCommand(connection, exactIds: false);
        return await ExecuteClaimAsync(
            command,
            instanceId,
            eventIds: null,
            batchSize,
            leaseDuration,
            ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchByIdsAsync(
        string instanceId,
        IReadOnlyList<string> eventIds,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentNullException.ThrowIfNull(eventIds);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        if (eventIds.Count == 0)
            return [];

        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = CreateClaimCommand(connection, exactIds: true);
        return await ExecuteClaimAsync(
            command,
            instanceId,
            eventIds,
            batchSize,
            leaseDuration,
            ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<RealtimeOutboxRecord>> ReadPreclaimedAsync(
        string instanceId,
        IReadOnlyList<string> eventIds,
        IReadOnlyList<string> claimTokens,
        int batchSize,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        ArgumentNullException.ThrowIfNull(eventIds);
        ArgumentNullException.ThrowIfNull(claimTokens);
        if (eventIds.Count == 0)
            return [];

        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = CreatePreclaimedReadCommand(connection);
        return await ExecutePreclaimedReadAsync(
            command,
            instanceId,
            eventIds,
            claimTokens,
            batchSize,
            ct).ConfigureAwait(false);
    }

    private NpgsqlCommand CreatePreclaimedReadCommand(NpgsqlConnection connection)
    {
        var command = new NpgsqlCommand(
            $"""
             WITH preclaimed AS MATERIALIZED (
                 SELECT candidate.event_id, candidate.event_type,
                        candidate.target_user_id, candidate.target_user_ids,
                        candidate.audience_kind, candidate.conversation_id,
                        candidate.payload_json, candidate.payload_utf8,
                        candidate.attempt_count, candidate.locked_by, candidate.claim_token,
                        candidate.trace_parent, candidate.trace_state, candidate.exclude_user_id,
                        candidate.status, candidate.published_at_ms,
                        candidate.next_attempt_at_ms, candidate.locked_until_ms
                 FROM UNNEST(@event_ids, @claim_tokens) AS claimed_hints(event_id, claim_token)
                 CROSS JOIN LATERAL (
                     SELECT item.*
                     FROM {_databaseSchema.OutboxTableSql} AS item
                     WHERE item.event_id = claimed_hints.event_id
                       AND item.claim_token = claimed_hints.claim_token
                       AND item.locked_by = @instance_id
                     LIMIT 1
                 ) AS candidate
             )
             SELECT item.event_id, item.event_type, item.target_user_id, item.target_user_ids,
                    item.audience_kind, item.conversation_id,
                    item.payload_json, item.payload_utf8, item.attempt_count, item.locked_by, item.claim_token,
                    item.trace_parent, item.trace_state, item.exclude_user_id
             FROM preclaimed AS item
             WHERE item.status = {(short)RealtimeOutboxStatus.Pending}
               AND item.published_at_ms IS NULL
               -- 预领取读取由 owner + claim_token + 有效租约授权；next_attempt_at 仅控制
               -- 无 owner 的恢复扫描。新插入的预领取行把 next_attempt_at 设为租约到期，
               -- 避免 recovery Pending 索引在有效租约期间反复读到 in-flight 行。
               AND item.locked_until_ms >= @now
             LIMIT @batch_size;
             """,
            connection);
        command.Parameters.Add("event_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        command.Parameters.Add("claim_tokens", NpgsqlDbType.Array | NpgsqlDbType.Text);
        command.Parameters.Add("instance_id", NpgsqlDbType.Text);
        command.Parameters.Add("now", NpgsqlDbType.Bigint);
        command.Parameters.Add("batch_size", NpgsqlDbType.Integer);
        return command;
    }

    private static Task<IReadOnlyList<RealtimeOutboxRecord>> ExecutePreclaimedReadAsync(
        NpgsqlCommand command,
        string instanceId,
        IReadOnlyList<string> eventIds,
        IReadOnlyList<string> claimTokens,
        int batchSize,
        CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        if (eventIds.Count != claimTokens.Count)
            throw new ArgumentException("Event IDs and claim tokens must have the same length.");
        if (eventIds.Count == 0)
            return Task.FromResult<IReadOnlyList<RealtimeOutboxRecord>>([]);

        command.Parameters["event_ids"].Value = eventIds;
        command.Parameters["claim_tokens"].Value = claimTokens;
        command.Parameters["instance_id"].Value = instanceId;
        command.Parameters["now"].Value = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        command.Parameters["batch_size"].Value = Math.Min(batchSize, eventIds.Count);
        return ReadClaimedRecordsAsync(command, batchSize, ct);
    }

    private NpgsqlCommand CreateClaimCommand(NpgsqlConnection connection, bool exactIds)
    {
        // 提示路径先只按主键锁行，再在物化结果上判断 retry/lease 资格。若把资格谓词
        // 放回目标表扫描，PostgreSQL 会在低速短表阶段选择 partial Pending 索引，随
        // Outbox churn 反复读取大量已删除 tuple。最终 UPDATE 也只做主键回表。
        var candidatesCteSql = exactIds
            ? $"""
               WITH locked_candidates AS MATERIALIZED (
                   SELECT candidate.event_id,
                          candidate.status,
                          candidate.published_at_ms,
                          candidate.next_attempt_at_ms,
                          candidate.locked_until_ms
                   FROM unnest(@event_ids) AS requested(event_id)
                   CROSS JOIN LATERAL (
                       SELECT item.event_id,
                              item.status,
                              item.published_at_ms,
                              item.next_attempt_at_ms,
                              item.locked_until_ms
                       FROM {_databaseSchema.OutboxTableSql} AS item
                       WHERE item.event_id = requested.event_id
                       FOR UPDATE OF item SKIP LOCKED
                       LIMIT 1
                   ) AS candidate
                   LIMIT @batch_size
               ),
               candidates AS MATERIALIZED (
                   SELECT candidate.event_id
                   FROM locked_candidates AS candidate
                   WHERE candidate.status = {(short)RealtimeOutboxStatus.Pending}
                     AND candidate.published_at_ms IS NULL
                     AND candidate.next_attempt_at_ms <= @now
                     AND (candidate.locked_until_ms IS NULL OR candidate.locked_until_ms < @now)
               )
               """
            : $"""
               WITH candidates AS MATERIALIZED (
                   SELECT item.event_id
                   FROM {_databaseSchema.OutboxTableSql} AS item
                   WHERE item.status = {(short)RealtimeOutboxStatus.Pending}
                     AND item.published_at_ms IS NULL
                     AND item.next_attempt_at_ms <= @now
                     AND (item.locked_until_ms IS NULL OR item.locked_until_ms < @now)
                   ORDER BY item.created_at_ms
                   FOR UPDATE OF item SKIP LOCKED
                   LIMIT @batch_size
               )
               """;
        var command = new NpgsqlCommand(
            $"""
             {candidatesCteSql}
             UPDATE {_databaseSchema.OutboxTableSql} AS item
             SET locked_by = @instance_id,
                 claim_token = @claim_token,
                 locked_until_ms = @locked_until,
                 attempt_count = item.attempt_count + 1
             FROM candidates
             WHERE item.event_id = candidates.event_id
             RETURNING item.event_id, item.event_type, item.target_user_id, item.target_user_ids,
                 item.audience_kind, item.conversation_id,
                 item.payload_json, item.payload_utf8, item.attempt_count, item.locked_by, item.claim_token,
                 item.trace_parent, item.trace_state, item.exclude_user_id;
             """,
            connection);
        if (exactIds)
            command.Parameters.Add("event_ids", NpgsqlDbType.Array | NpgsqlDbType.Text);
        command.Parameters.Add("now", NpgsqlDbType.Bigint);
        command.Parameters.Add("batch_size", NpgsqlDbType.Integer);
        command.Parameters.Add("instance_id", NpgsqlDbType.Text);
        command.Parameters.Add("claim_token", NpgsqlDbType.Text);
        command.Parameters.Add("locked_until", NpgsqlDbType.Bigint);
        return command;
    }

    private static async Task<IReadOnlyList<RealtimeOutboxRecord>> ExecuteClaimAsync(
        NpgsqlCommand command,
        string instanceId,
        IReadOnlyList<string>? eventIds,
        int batchSize,
        TimeSpan leaseDuration,
        CancellationToken ct)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(batchSize);
        if (leaseDuration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(leaseDuration), "Lease duration must be positive.");
        if (eventIds is { Count: 0 })
            return [];

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (eventIds is not null)
            command.Parameters["event_ids"].Value = eventIds;
        command.Parameters["now"].Value = now;
        command.Parameters["batch_size"].Value = eventIds is null
            ? batchSize
            : Math.Min(batchSize, eventIds.Count);
        command.Parameters["instance_id"].Value = instanceId;
        command.Parameters["claim_token"].Value = Guid.NewGuid().ToString("N");
        command.Parameters["locked_until"].Value = now + (long)leaseDuration.TotalMilliseconds;
        return await ReadClaimedRecordsAsync(command, batchSize, ct).ConfigureAwait(false);
    }

    private sealed class ClaimSession : IRealtimeOutboxClaimSession
    {
        private readonly NpgsqlConnection _connection;
        private readonly string _instanceId;
        private readonly NpgsqlCommand _scanCommand;
        private readonly NpgsqlCommand _exactCommand;
        private readonly NpgsqlCommand _preclaimedCommand;
        private int _disposed;

        public ClaimSession(
            NpgsqlRealtimeOutboxStore owner,
            NpgsqlConnection connection,
            string instanceId)
        {
            _connection = connection;
            _instanceId = instanceId;
            _scanCommand = owner.CreateClaimCommand(connection, exactIds: false);
            _exactCommand = owner.CreateClaimCommand(connection, exactIds: true);
            _preclaimedCommand = owner.CreatePreclaimedReadCommand(connection);
        }

        public async Task PrepareAsync(CancellationToken ct)
        {
            await _scanCommand.PrepareAsync(ct).ConfigureAwait(false);
            await _exactCommand.PrepareAsync(ct).ConfigureAwait(false);
            await _preclaimedCommand.PrepareAsync(ct).ConfigureAwait(false);
        }

        public Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchAsync(
            int batchSize,
            TimeSpan leaseDuration,
            CancellationToken ct = default)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            return ExecuteClaimAsync(
                _scanCommand,
                _instanceId,
                eventIds: null,
                batchSize,
                leaseDuration,
                ct);
        }

        public Task<IReadOnlyList<RealtimeOutboxRecord>> ClaimBatchByIdsAsync(
            IReadOnlyList<string> eventIds,
            int batchSize,
            TimeSpan leaseDuration,
            CancellationToken ct = default)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            ArgumentNullException.ThrowIfNull(eventIds);
            return ExecuteClaimAsync(
                _exactCommand,
                _instanceId,
                eventIds,
                batchSize,
                leaseDuration,
                ct);
        }

        public Task<IReadOnlyList<RealtimeOutboxRecord>> ReadPreclaimedAsync(
            IReadOnlyList<string> eventIds,
            IReadOnlyList<string> claimTokens,
            int batchSize,
            CancellationToken ct = default)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            ArgumentNullException.ThrowIfNull(eventIds);
            ArgumentNullException.ThrowIfNull(claimTokens);
            return ExecutePreclaimedReadAsync(
                _preclaimedCommand,
                _instanceId,
                eventIds,
                claimTokens,
                batchSize,
                ct);
        }

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;

            await _scanCommand.DisposeAsync().ConfigureAwait(false);
            await _exactCommand.DisposeAsync().ConfigureAwait(false);
            await _preclaimedCommand.DisposeAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task<IReadOnlyList<RealtimeOutboxRecord>> ReadClaimedRecordsAsync(
        NpgsqlCommand command,
        int batchSize,
        CancellationToken ct)
    {
        var records = new List<RealtimeOutboxRecord>(Math.Min(batchSize, 16));
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            records.Add(ReadClaimedRecord(reader));

        return records;
    }

    private static RealtimeOutboxRecord ReadClaimedRecord(NpgsqlDataReader reader)
    {
        // 数据库列是投递目标的唯一权威，不再从新格式 payload 反序列化路由信息。
        var eventId = reader.GetString(0);
        var eventType = (RealtimeEventType)reader.GetInt16(1);
        var targetUserId = reader.GetInt64(2);
        long[]? targetUserIds = null;
        if (!reader.IsDBNull(3))
            targetUserIds = reader.GetFieldValue<long[]>(3);
        var audienceKindRaw = reader.IsDBNull(4) ? (short)0 : reader.GetInt16(4);
        var conversationId = reader.IsDBNull(5) ? null : reader.GetString(5);
        var payloadJson = reader.IsDBNull(6) ? null : reader.GetString(6);
        ReadOnlyMemory<byte>? payloadUtf8 = null;
        if (!reader.IsDBNull(7))
            payloadUtf8 = reader.GetFieldValue<byte[]>(7);
        var attemptCount = reader.GetInt32(8);
        var lockOwner = reader.GetString(9);
        var claimToken = reader.GetString(10);
        var traceParent = reader.IsDBNull(11) ? null : reader.GetString(11);
        var traceState = reader.IsDBNull(12) ? null : reader.GetString(12);
        long? excludeUserId = reader.IsDBNull(13) ? null : reader.GetInt64(13);

        RealtimeEvent? evt = null;
        if (payloadUtf8 is not { Length: > 0 } && payloadJson is not null)
        {
            // 兼容迁移前仅有 payload_json 的历史记录；新记录完全跳过 JSON 解析。
            evt = JsonSerializer.Deserialize(
                payloadJson,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);
            if (evt is not null)
            {
                if (targetUserIds is null && evt.TargetUserIds is { Length: > 0 })
                    targetUserIds = evt.TargetUserIds;
                if (audienceKindRaw == 0 && evt.AudienceKind is not null)
                    audienceKindRaw = (short)evt.AudienceKind.Value;
                excludeUserId ??= evt.ExcludeUserId;
                traceParent ??= evt.TraceParent;
                traceState ??= evt.TraceState;
                payloadUtf8 = RealtimeEventWireSerializer.SerializeToUtf8Bytes(
                    CreateWirePayload(evt));
            }
        }

        return new RealtimeOutboxRecord(
            eventId,
            eventType,
            targetUserId,
            targetUserIds,
            audienceKindRaw == 0 ? null : (AudienceKind)audienceKindRaw,
            conversationId,
            excludeUserId,
            traceParent,
            traceState,
            evt,
            attemptCount,
            lockOwner,
            claimToken,
            payloadUtf8);
    }

    /// <summary>
    /// 四-1：创建 wire payload 副本。P0-5：保留 AudienceKind 与 ConversationId，
    /// 仅排除 TargetUserIds（O(N) 数组）。用于旧记录回退时生成 payload_utf8。
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
            AudienceKind = evt.AudienceKind,
            ConversationId = evt.ConversationId,
            // 极限-3：ExcludeUserId 保留在 wire payload 中，Gateway 据此跳过排除用户。
            ExcludeUserId = evt.ExcludeUserId,
            ProtocolVersion = evt.ProtocolVersion,
            AudienceVersion = evt.AudienceVersion,
            MinProtocolVersion = evt.MinProtocolVersion,
            TargetUserIds = null,
            Payload = null,
        };
    }

    public Task MarkPublishedAsync(RealtimeOutboxRecord record, CancellationToken ct = default) =>
        UpdateWithoutResultAsync(
            record,
            $"""
             published_at_ms = @now,
             status = {(short)RealtimeOutboxStatus.Published},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = NULL,
             claim_token = NULL
             """,
            null,
            ct);

    public Task<int> TryMarkPublishedAsync(
        RealtimeOutboxRecord record,
        CancellationToken ct = default) =>
        UpdateAsync(
            record,
            $"""
             published_at_ms = @now,
             status = {(short)RealtimeOutboxStatus.Published},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = NULL,
             claim_token = NULL
             """,
            null,
            ct);

    public async Task<int> DeleteClaimedPublishedAsync(
        RealtimeOutboxRecord record,
        CancellationToken ct = default)
    {
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql}
             WHERE event_id = @event_id
               AND claim_token = @claim_token
               AND status = {(short)RealtimeOutboxStatus.Pending};
             """,
            connection);
        command.Parameters.AddWithValue("event_id", record.EventId);
        command.Parameters.AddWithValue("claim_token", record.ClaimToken);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public Task MarkFailedAsync(
        RealtimeOutboxRecord record,
        string error,
        TimeSpan retryDelay,
        CancellationToken ct = default) =>
        UpdateWithoutResultAsync(
            record,
            "next_attempt_at_ms = @next_attempt, locked_by = NULL, locked_until_ms = NULL, last_error = @error, claim_token = NULL",
            (error.Length <= 2048 ? error : error[..2048], retryDelay),
            ct);

    public Task MarkDeadAsync(
        RealtimeOutboxRecord record,
        string error,
        CancellationToken ct = default) =>
        UpdateWithoutResultAsync(
            record,
            $"""
             status = {(short)RealtimeOutboxStatus.Dead},
             locked_by = NULL,
             locked_until_ms = NULL,
             last_error = @error,
             next_attempt_at_ms = @now,
             claim_token = NULL
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

    /// <summary>
    /// OUTBOX-RECOVERY-1-3：带审计的死信重放。在单个事务内锁定并读取 Dead 行，
    /// 重置为 Pending 并写入 <c>outbox_replay_audit</c>。未找到 Dead 行时事务回滚、
    /// 不产生审计记录，返回 false；仅当 Dead 行被成功重置且审计写入成功才提交。
    /// </summary>
    public async Task<bool> ReplayDeadWithAuditAsync(
        string eventId,
        string @operator,
        string? reason,
        CancellationToken ct = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(eventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(@operator);
        eventId = eventId.Trim();
        @operator = @operator.Trim();
        if (@operator.Length > 128)
            throw new ArgumentException("操作者标识过长（上限 128）。", nameof(@operator));
        if (reason is { Length: > 512 })
            reason = reason[..512];

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        // 1) 锁定并读取 Dead 行的历史（attempt_count / last_error），确保并发重放串行化。
        int? priorAttemptCount;
        string? priorLastError;
        bool found;
        await using (var readCommand = new NpgsqlCommand(
            $"""
             SELECT attempt_count, last_error
             FROM {_databaseSchema.OutboxTableSql}
             WHERE event_id = @event_id
               AND status = {(short)RealtimeOutboxStatus.Dead}
             FOR UPDATE
             """,
            connection,
            transaction))
        {
            readCommand.Parameters.AddWithValue("event_id", eventId);
            await using var reader = await readCommand.ExecuteReaderAsync(ct).ConfigureAwait(false);
            found = await reader.ReadAsync(ct).ConfigureAwait(false);
            if (found)
            {
                priorAttemptCount = reader.GetInt32(0);
                priorLastError = reader.IsDBNull(1) ? null : reader.GetString(1);
            }
            else
            {
                priorAttemptCount = null;
                priorLastError = null;
            }
        }

        // 必须在 reader 释放后再回滚，避免 "command already in progress"。
        if (!found)
        {
            await transaction.RollbackAsync(ct).ConfigureAwait(false);
            _metrics?.RecordOutboxReplayResult("not_found");
            return false;
        }

        // 2) 重置为 Pending（与 ReplayDeadBatchAsync 相同的字段语义）。
        await using (var resetCommand = new NpgsqlCommand(
            $"""
             UPDATE {_databaseSchema.OutboxTableSql}
             SET status = {(short)RealtimeOutboxStatus.Pending},
                 published_at_ms = NULL,
                 attempt_count = 0,
                 next_attempt_at_ms = @now,
                 locked_by = NULL,
                 locked_until_ms = NULL,
                 last_error = NULL
             WHERE event_id = @event_id
             """,
            connection,
            transaction))
        {
            resetCommand.Parameters.AddWithValue("now", now);
            resetCommand.Parameters.AddWithValue("event_id", eventId);
            await resetCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        // 3) 写入审计记录（原 event id、尝试次数、失败分类、操作者/原因、新 checkpoint）。
        await using (var auditCommand = new NpgsqlCommand(
            $"""
             INSERT INTO {_databaseSchema.OutboxReplayAuditTableSql}
                 ("event_id", "replayed_at_ms", "prior_attempt_count",
                  "prior_last_error", "operator", "reason", "new_checkpoint_ms")
             VALUES (@event_id, @replayed_at, @prior_attempt_count,
                     @prior_last_error, @operator, @reason, @new_checkpoint)
             """,
            connection,
            transaction))
        {
            auditCommand.Parameters.AddWithValue("event_id", eventId);
            auditCommand.Parameters.AddWithValue("replayed_at", now);
            auditCommand.Parameters.AddWithValue("prior_attempt_count", priorAttemptCount!.Value);
            auditCommand.Parameters.AddWithValue(
                "prior_last_error",
                priorLastError is null ? DBNull.Value : priorLastError);
            auditCommand.Parameters.AddWithValue("operator", @operator);
            auditCommand.Parameters.AddWithValue(
                "reason",
                reason is null ? DBNull.Value : reason);
            auditCommand.Parameters.AddWithValue("new_checkpoint", now);
            await auditCommand.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        }

        await transaction.CommitAsync(ct).ConfigureAwait(false);
        _metrics?.RecordOutboxReplayResult("success");
        return true;
    }

    /// <summary>
    /// OUTBOX-RECOVERY-1-3：分页查询死信重放审计记录，按重放时间倒序。
    /// </summary>
    public async Task<IReadOnlyList<RealtimeOutboxReplayAudit>> ListReplayAuditsAsync(
        string? eventId,
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
             SELECT "event_id", "replayed_at_ms", "prior_attempt_count",
                    "prior_last_error", "operator", "reason", "new_checkpoint_ms"
             FROM {_databaseSchema.OutboxReplayAuditTableSql}
             WHERE (@event_id IS NULL OR "event_id" = @event_id)
             ORDER BY "replayed_at_ms" DESC
             OFFSET @offset
             LIMIT @limit;
             """,
            connection);
        command.Parameters.AddWithValue("offset", offset);
        command.Parameters.AddWithValue("limit", limit);
        var eventIdParam = command.Parameters.Add("event_id", NpgsqlTypes.NpgsqlDbType.Text);
        eventIdParam.Value = string.IsNullOrWhiteSpace(eventId) ? DBNull.Value : eventId.Trim();

        var items = new List<RealtimeOutboxReplayAudit>(limit);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new RealtimeOutboxReplayAudit(
                reader.GetString(0),
                reader.GetInt64(1),
                reader.GetInt32(2),
                reader.IsDBNull(3) ? null : reader.GetString(3),
                reader.GetString(4),
                reader.IsDBNull(5) ? null : reader.GetString(5),
                reader.GetInt64(6)));
        }

        return items;
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
        // 仅扫 Pending / Dead；Pending 峰值通常很小。MAX(attempt_count) 有意不再维护
        // 热路径索引，让 claim 的 attempt_count/lease 更新可以走 HOT，避免每消息索引重写。
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

    public async Task<int> DeleteClaimedPublishedBatchAsync(
        IReadOnlyList<RealtimeOutboxRecord> records,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(records);
        if (records.Count == 0)
            return 0;

        var eventIds = records.Select(static record => record.EventId).ToArray();
        var claimTokens = records.Select(static record => record.ClaimToken).ToArray();
        await using var connection = await _databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(
            $"""
             DELETE FROM {_databaseSchema.OutboxTableSql} AS item
             USING UNNEST(@event_ids, @claim_tokens) AS arr(event_id, claim_token)
             WHERE item.event_id = arr.event_id
               AND item.claim_token = arr.claim_token
               AND item.status = {(short)RealtimeOutboxStatus.Pending};
             """,
            connection);
        command.Parameters.AddWithValue("event_ids", eventIds);
        command.Parameters.AddWithValue("claim_tokens", claimTokens);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

    private async Task UpdateWithoutResultAsync(
        RealtimeOutboxRecord record,
        string setClause,
        (string Error, TimeSpan Delay)? failure,
        CancellationToken ct)
    {
        _ = await UpdateAsync(record, setClause, failure, ct).ConfigureAwait(false);
    }

    private async Task<int> UpdateAsync(
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

        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
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

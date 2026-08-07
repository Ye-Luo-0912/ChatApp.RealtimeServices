using System.Collections.Concurrent;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Outbox;

/// <summary>
/// Perf-9：Outbox 批量写入的统一 SQL 助手。
/// 消除 <see cref="PostgresOutboxWriter"/>、<see cref="Stores.NpgsqlRealtimeReactionStore"/>、
/// <see cref="Stores.NpgsqlRealtimeConversationStore"/> 中三份重复的 UNNEST INSERT 实现，
/// 并修复旧 <c>NpgsqlRealtimeConversationStore.InsertOutboxAsync</c> 遗漏 <c>target_user_ids</c> 列的问题。
/// </summary>
/// <remarks>
/// Perf-7：标量批使用固定命令文本 + UNNEST，避免动态 VALUES 列表污染 Npgsql AutoPrepare 缓存。
/// <para>
/// 极限-6：移除 <c>string.Join</c> + <c>string_to_array</c> 文本编码。事件按是否携带
/// <see cref="RealtimeEvent.TargetUserIds"/> 拆分两条路径：
/// <list type="bullet">
/// <item>标量事件（无 TargetUserIds，占绝大多数）：UNNEST 批量 INSERT，
/// <c>target_user_ids</c> 列写 NULL，零文本编码开销。</item>
/// <item>数组事件（单聊双目标、MembersAdded 等多用户事件）：逐行 INSERT，
/// Npgsql 直接将 <c>long[]</c> 映射为 <c>bigint[]</c> 参数，原生二进制传输。</item>
/// </list>
/// 群消息广播（AudienceKind=Conversation）走标量路径，<c>target_user_ids</c> 始终为 NULL——
/// Conversation Audience 完成后，群消息根本不需要数组。
/// </para>
/// <para>
/// 四-1/五：预序列化为 UTF-8 字节写入 <c>payload_utf8</c> 列。P0-5：wire payload 保留
/// <see cref="RealtimeEvent.AudienceKind"/> 与 <see cref="RealtimeEvent.ConversationId"/>
/// 供 Gateway 路由判断；仅排除 <see cref="RealtimeEvent.TargetUserIds"/>（O(N) 数组，数据库列为唯一权威）。
/// <c>payload_json</c> 列写入 <c>NULL</c>，停止双写；Claim 路径仅读取列 + <c>payload_utf8</c>。
/// </para>
/// </remarks>
internal static class OutboxInsertHelper
{
    private const string ArrayInsertCommandText = """
        INSERT INTO {0} (
            event_id, payload_json, payload_utf8, target_user_id, event_type, status,
            created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
            audience_kind, conversation_id, exclude_user_id, trace_parent, trace_state, occurred_at_ms
        )
        VALUES (
            $1, NULL, $2, $3, $4, $5,
            $6, $7, 0, $8,
            $9, $10, NULLIF($11, 0), $12, $13, $14
        )
        ON CONFLICT (event_id) DO NOTHING;
        """;

    private static readonly ConcurrentDictionary<string, string> ArrayInsertCommandTexts =
        new(StringComparer.Ordinal);

    /// <summary>
    /// 在指定事务内批量写入 Outbox 事件（<c>ON CONFLICT (event_id) DO NOTHING</c> 幂等）。
    /// 支持聚合事件（<see cref="RealtimeEvent.TargetUserIds"/> 非空时写入 target_user_ids 列）。
    /// </summary>
    /// <returns>实际写入的行数（扣除 <c>ON CONFLICT DO NOTHING</c> 跳过的重复行）。
    /// Reliability-4：调用方应在事务提交成功后用该值调用 <c>RecordOutboxEnqueued</c>，
    /// 避免传入事件数量与实际插入数量不一致导致 <c>realtime.outbox.pending</c> 长期漂移。</returns>
    public static async Task<int> InsertManyAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyList<RealtimeEvent> events,
        CancellationToken ct)
    {
        if (events.Count == 0)
            return 0;

        // 极限-6：按是否携带 TargetUserIds 拆分两条路径，移除文本编码。
        // 标量事件（绝大多数）走 UNNEST 批量；数组事件（低频）走逐行原生 bigint[] 参数。
        List<RealtimeEvent>? scalarEvents = null;
        List<RealtimeEvent>? arrayEvents = null;
        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            if (evt.TargetUserIds is { Length: > 0 })
                (arrayEvents ??= new List<RealtimeEvent>(4)).Add(evt);
            else
                (scalarEvents ??= new List<RealtimeEvent>(events.Count)).Add(evt);
        }

        // 单次取 now，保证两条路径 created_at_ms / next_attempt_at_ms 一致。
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var inserted = 0;
        if (scalarEvents is not null)
            inserted += await InsertScalarBatchAsync(connection, transaction, schema, scalarEvents, now, ct).ConfigureAwait(false);
        if (arrayEvents is not null)
            inserted += await InsertArrayBatchAsync(connection, transaction, schema, arrayEvents, now, ct).ConfigureAwait(false);
        return inserted;
    }

    /// <summary>
    /// 极限-6：标量事件批量写入（TargetUserIds 为空的事件）。
    /// 使用 UNNEST 单条 SQL，<c>target_user_ids</c> 列写 NULL。
    /// 群广播（AudienceKind=Conversation）与其他单目标事件走此路径；单聊消息使用双目标数组路径。
    /// </summary>
    private static async Task<int> InsertScalarBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyList<RealtimeEvent> events,
        long now,
        CancellationToken ct)
    {
        var eventIds = new string[events.Count];
        var payloadUtf8s = new byte[events.Count][];
        var targetUserIds = new long[events.Count];
        var eventTypes = new short[events.Count];
        var audienceKinds = new short[events.Count];
        var conversationIds = new string?[events.Count];
        var excludeUserIds = new long[events.Count];
        var traceParents = new string?[events.Count];
        var traceStates = new string?[events.Count];
        var occurredAtMsValues = new long[events.Count];

        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            eventIds[i] = evt.EventId;
            var wireEvt = CreateWirePayload(evt);
            payloadUtf8s[i] = JsonSerializer.SerializeToUtf8Bytes(
                wireEvt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);
            targetUserIds[i] = evt.TargetUserId;
            eventTypes[i] = (short)evt.Type;
            audienceKinds[i] = (short)(evt.AudienceKind ?? 0);
            conversationIds[i] = evt.ConversationId;
            excludeUserIds[i] = evt.ExcludeUserId ?? 0L;
            traceParents[i] = evt.TraceParent;
            traceStates[i] = evt.TraceState;
            occurredAtMsValues[i] = evt.OccurredAtMs;
        }

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
                 audience_kind, conversation_id, exclude_user_id, trace_parent, trace_state, occurred_at_ms
             )
             SELECT
                 arr.event_id,
                 NULL,
                 arr.payload_utf8,
                 arr.target_user_id,
                 arr.event_type,
                 $1,
                 $2,
                 $3,
                 0,
                 NULL,
                 arr.audience_kind,
                 arr.conversation_id,
                 NULLIF(arr.exclude_user_id, 0),
                 arr.trace_parent,
                 arr.trace_state,
                 arr.occurred_at_ms
             FROM UNNEST(
                 $4,
                 $5,
                 $6,
                 $7,
                 $8,
                 $10,
                 $9,
                 $11,
                 $12,
                 $13
             ) AS arr(event_id, payload_utf8, target_user_id, event_type, audience_kind, conversation_id, exclude_user_id, trace_parent, trace_state, occurred_at_ms)
             ON CONFLICT (event_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue((short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue(now);
        command.Parameters.AddWithValue(now);
        command.Parameters.AddWithValue(eventIds);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Bytea, payloadUtf8s);
        command.Parameters.AddWithValue(targetUserIds);
        command.Parameters.AddWithValue(eventTypes);
        command.Parameters.AddWithValue(audienceKinds);
        command.Parameters.AddWithValue(excludeUserIds);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, conversationIds);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, traceParents);
        command.Parameters.AddWithValue(NpgsqlDbType.Array | NpgsqlDbType.Text, traceStates);
        command.Parameters.AddWithValue(occurredAtMsValues);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 极限-6：数组事件逐行写入（TargetUserIds 非空的事件）。
    /// Npgsql 直接将 <c>long[]</c> 映射为 <c>bigint[]</c> 参数，原生二进制传输，
    /// 替代旧的 <c>string.Join</c> + <c>string_to_array</c> 文本编码。
    /// 单事件入口会绕过临时 List 分组，直接执行一条参数化 INSERT。
    /// </summary>
    private static async Task<int> InsertArrayBatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        IReadOnlyList<RealtimeEvent> events,
        long now,
        CancellationToken ct)
    {
        var inserted = 0;
        foreach (var evt in events)
        {
            inserted += await InsertArrayAsync(connection, transaction, schema, evt, now, ct)
                .ConfigureAwait(false);
        }
        return inserted;
    }

    private static async Task<int> InsertArrayAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        RealtimeEvent evt,
        long now,
        CancellationToken ct)
    {
        var formattedCommandText = ArrayInsertCommandTexts.GetOrAdd(
            schema.OutboxTableSql,
            static table => ArrayInsertCommandText.Replace("{0}", table, StringComparison.Ordinal));
        var wireEvt = CreateWirePayload(evt);
        var payloadUtf8 = JsonSerializer.SerializeToUtf8Bytes(
            wireEvt,
            RealtimeJsonSerializerContext.Default.RealtimeEvent);

        await using var command = new NpgsqlCommand(formattedCommandText, connection, transaction);
        command.Parameters.AddWithValue(evt.EventId);
        command.Parameters.AddWithValue(NpgsqlDbType.Bytea, payloadUtf8);
        command.Parameters.AddWithValue(evt.TargetUserId);
        command.Parameters.AddWithValue((short)evt.Type);
        command.Parameters.AddWithValue((short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue(now);
        command.Parameters.AddWithValue(now);
        // 极限-6：原生 bigint[] 参数，Npgsql 直接映射 long[] → bigint[]，无需文本编码。
        command.Parameters.AddWithValue(
            NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            evt.TargetUserIds!);
        command.Parameters.AddWithValue((short)(evt.AudienceKind ?? 0));
        command.Parameters.AddWithValue((object?)evt.ConversationId ?? DBNull.Value);
        command.Parameters.AddWithValue(evt.ExcludeUserId ?? 0L);
        command.Parameters.AddWithValue((object?)evt.TraceParent ?? DBNull.Value);
        command.Parameters.AddWithValue((object?)evt.TraceState ?? DBNull.Value);
        command.Parameters.AddWithValue(evt.OccurredAtMs);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>
    /// 四-1：创建用于 wire payload 的副本。
    /// P0-5：保留 <see cref="RealtimeEvent.AudienceKind"/> 与 <see cref="RealtimeEvent.ConversationId"/>
    /// 在 wire payload 中，Gateway 需要这些字段判断投递语义（会话级受众 vs 普通用户事件）。
    /// <see cref="RealtimeEvent.TargetUserIds"/> 仍排除：O(N) 数组不在 payload 中，数据库列是唯一权威。
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
            // P0-5：AudienceKind 和 ConversationId 保留在 wire payload 中，
            // Gateway 需要这些字段判断投递语义。
            AudienceKind = evt.AudienceKind,
            ConversationId = evt.ConversationId,
            // 极限-3：ExcludeUserId 保留在 wire payload 中。
            ExcludeUserId = evt.ExcludeUserId,
            ProtocolVersion = evt.ProtocolVersion,
            AudienceVersion = evt.AudienceVersion,
            MinProtocolVersion = evt.MinProtocolVersion,
            // TargetUserIds 仍排除：O(N) 数组不在 payload 中，数据库列是唯一权威。
            TargetUserIds = null,
            // Payload 是 [JsonIgnore] 的运行时引用，不参与序列化，置空避免携带。
            Payload = null,
        };
    }

    /// <summary>单条事件的便捷包装。</summary>
    public static async Task<int> InsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        RealtimeEvent evt,
        CancellationToken ct)
    {
        if (evt.TargetUserIds is { Length: > 0 })
        {
            var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            return await InsertArrayAsync(
                    connection,
                    transaction,
                    schema,
                    evt,
                    now,
                    ct)
                .ConfigureAwait(false);
        }

        return await InsertManyAsync(connection, transaction, schema, [evt], ct)
            .ConfigureAwait(false);
    }
}

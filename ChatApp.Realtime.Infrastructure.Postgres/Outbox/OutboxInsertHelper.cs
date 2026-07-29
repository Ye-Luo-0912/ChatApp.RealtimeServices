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
/// Perf-7：使用固定命令文本 + UNNEST，避免动态 VALUES 列表污染 Npgsql AutoPrepare 缓存。
/// <c>target_user_ids</c> 是每行的 <c>bigint[]</c>，Npgsql 不直接支持锯齿数组，
/// 因此将其编码为逗号分隔文本，在 SQL 中用 <c>string_to_array</c> 解码。
/// <para>
/// 四-1/五：预序列化为 UTF-8 字节写入 <c>payload_utf8</c> 列。P0-5：wire payload 保留
/// <see cref="RealtimeEvent.AudienceKind"/> 与 <see cref="RealtimeEvent.ConversationId"/>
/// 供 Gateway 路由判断；仅排除 <see cref="RealtimeEvent.TargetUserIds"/>（O(N) 数组，数据库列为唯一权威）。
/// <c>payload_json</c> 列写入 <c>NULL</c>，停止双写；Claim 路径仅读取列 + <c>payload_utf8</c>。
/// </para>
/// </remarks>
internal static class OutboxInsertHelper
{
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

        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var eventIds = new string[events.Count];
        // 四-1/五：预序列化的 UTF-8 字节（排除路由字段），Publisher 直接发送避免重新序列化。
        var payloadUtf8s = new byte[events.Count][];
        var targetUserIds = new long[events.Count];
        var eventTypes = new short[events.Count];
        var targetUserIdsText = new string?[events.Count];
        // Perf-2：会话级受众路由字段。audience_kind 默认 0=User，conversation_id 可空。
        var audienceKinds = new short[events.Count];
        var conversationIds = new string?[events.Count];
        // P0-8：trace context 与事件时间戳持久化到独立列，Claim 路径零解析读取。
        var traceParents = new string?[events.Count];
        var traceStates = new string?[events.Count];
        var occurredAtMsValues = new long[events.Count];

        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            eventIds[i] = evt.EventId;
            // 四-1：序列化 wire payload。P0-5：保留 AudienceKind 与 ConversationId 供 Gateway 路由，
            // 仅排除 TargetUserIds（O(N) 数组，数据库列为唯一权威）。
            var wireEvt = CreateWirePayload(evt);
            payloadUtf8s[i] = JsonSerializer.SerializeToUtf8Bytes(
                wireEvt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);
            targetUserIds[i] = evt.TargetUserId;
            eventTypes[i] = (short)evt.Type;
            // 编码 TargetUserIds 为逗号分隔文本；NULL 或空数组 → null。
            targetUserIdsText[i] = evt.TargetUserIds is { Length: > 0 }
                ? string.Join(",", evt.TargetUserIds)
                : null;
            // AudienceKind 为 null 时按 User(0) 持久化，兼容历史事件。
            audienceKinds[i] = (short)(evt.AudienceKind ?? 0);
            conversationIds[i] = evt.ConversationId;
            // P0-8：trace context 写入独立列。
            traceParents[i] = evt.TraceParent;
            traceStates[i] = evt.TraceState;
            occurredAtMsValues[i] = evt.OccurredAtMs;
        }

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
                 audience_kind, conversation_id, trace_parent, trace_state, occurred_at_ms
             )
             SELECT
                 arr.event_id,
                 NULL,
                 arr.payload_utf8,
                 arr.target_user_id,
                 arr.event_type,
                 @status,
                 @created_at_ms,
                 @next_attempt_at_ms,
                 0,
                 CASE
                     WHEN arr.target_user_ids_text IS NULL OR arr.target_user_ids_text = ''
                     THEN NULL
                     ELSE string_to_array(arr.target_user_ids_text, ',')::bigint[]
                 END,
                 arr.audience_kind,
                 arr.conversation_id,
                 arr.trace_parent,
                 arr.trace_state,
                 arr.occurred_at_ms
             FROM UNNEST(
                 @event_ids,
                 @payload_utf8s,
                 @target_user_ids,
                 @event_types,
                 @target_user_ids_text,
                 @audience_kinds,
                 @conversation_ids,
                 @trace_parents,
                 @trace_states,
                 @occurred_at_ms_values
             ) AS arr(event_id, payload_utf8, target_user_id, event_type, target_user_ids_text, audience_kind, conversation_id, trace_parent, trace_state, occurred_at_ms)
             ON CONFLICT (event_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        command.Parameters.AddWithValue("event_ids", eventIds);
        var payloadUtf8Param = command.Parameters.Add(
            "payload_utf8s",
            NpgsqlDbType.Array | NpgsqlDbType.Bytea);
        payloadUtf8Param.Value = payloadUtf8s;
        command.Parameters.AddWithValue("target_user_ids", targetUserIds);
        command.Parameters.AddWithValue("event_types", eventTypes);
        command.Parameters.AddWithValue("audience_kinds", audienceKinds);
        var targetUserIdsTextParam = command.Parameters.Add(
            "target_user_ids_text",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        targetUserIdsTextParam.Value = targetUserIdsText;
        var conversationIdsParam = command.Parameters.Add(
            "conversation_ids",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        conversationIdsParam.Value = conversationIds;
        // P0-8：trace context 与 occurred_at_ms 列参数。
        var traceParentsParam = command.Parameters.Add(
            "trace_parents",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        traceParentsParam.Value = traceParents;
        var traceStatesParam = command.Parameters.Add(
            "trace_states",
            NpgsqlDbType.Array | NpgsqlDbType.Text);
        traceStatesParam.Value = traceStates;
        command.Parameters.AddWithValue("occurred_at_ms_values", occurredAtMsValues);
        // ExecuteNonQueryAsync 返回受影响行数；ON CONFLICT DO NOTHING 跳过的行不计入。
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
            // TargetUserIds 仍排除：O(N) 数组不在 payload 中，数据库列是唯一权威。
            TargetUserIds = null,
            // Payload 是 [JsonIgnore] 的运行时引用，不参与序列化，置空避免携带。
            Payload = null,
        };
    }

    /// <summary>单条事件的便捷包装。</summary>
    public static Task<int> InsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        RealtimeEvent evt,
        CancellationToken ct) => InsertManyAsync(connection, transaction, schema, [evt], ct);
}

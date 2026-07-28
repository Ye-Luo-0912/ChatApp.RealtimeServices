using System.Text;
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
/// Perf-4：预序列化为 UTF-8 字节写入 <c>payload_utf8</c> 列，Publisher 直接发送避免重新序列化；
/// 同时从 UTF-8 字节解码为字符串写入 <c>payload_json</c> 列，兼容旧代码与查询。
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
        var payloadJsons = new string[events.Count];
        // Perf-4：预序列化的 UTF-8 字节，Publisher 直接发送避免重新序列化。
        var payloadUtf8s = new byte[events.Count][];
        var targetUserIds = new long[events.Count];
        var eventTypes = new short[events.Count];
        var targetUserIdsText = new string?[events.Count];
        // Perf-2：会话级受众路由字段。audience_kind 默认 0=User，conversation_id 可空。
        var audienceKinds = new short[events.Count];
        var conversationIds = new string?[events.Count];

        for (var i = 0; i < events.Count; i++)
        {
            var evt = events[i];
            eventIds[i] = evt.EventId;
            // Perf-4：一次序列化为 UTF-8 字节，同时填充 payload_json 与 payload_utf8。
            var utf8Bytes = JsonSerializer.SerializeToUtf8Bytes(
                evt,
                RealtimeJsonSerializerContext.Default.RealtimeEvent);
            payloadUtf8s[i] = utf8Bytes;
            payloadJsons[i] = Encoding.UTF8.GetString(utf8Bytes);
            targetUserIds[i] = evt.TargetUserId;
            eventTypes[i] = (short)evt.Type;
            // 编码 TargetUserIds 为逗号分隔文本；NULL 或空数组 → null。
            targetUserIdsText[i] = evt.TargetUserIds is { Length: > 0 }
                ? string.Join(",", evt.TargetUserIds)
                : null;
            // AudienceKind 为 null 时按 User(0) 持久化，兼容历史事件。
            audienceKinds[i] = (short)(evt.AudienceKind ?? 0);
            conversationIds[i] = evt.ConversationId;
        }

        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.OutboxTableSql} (
                 event_id, payload_json, payload_utf8, target_user_id, event_type, status,
                 created_at_ms, next_attempt_at_ms, attempt_count, target_user_ids,
                 audience_kind, conversation_id
             )
             SELECT
                 arr.event_id,
                 arr.payload_json,
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
                 arr.conversation_id
             FROM UNNEST(
                 @event_ids,
                 @payload_jsons,
                 @payload_utf8s,
                 @target_user_ids,
                 @event_types,
                 @target_user_ids_text,
                 @audience_kinds,
                 @conversation_ids
             ) AS arr(event_id, payload_json, payload_utf8, target_user_id, event_type, target_user_ids_text, audience_kind, conversation_id)
             ON CONFLICT (event_id) DO NOTHING;
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("status", (short)RealtimeOutboxStatus.Pending);
        command.Parameters.AddWithValue("created_at_ms", now);
        command.Parameters.AddWithValue("next_attempt_at_ms", now);
        command.Parameters.AddWithValue("event_ids", eventIds);
        command.Parameters.AddWithValue("payload_jsons", payloadJsons);
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
        // ExecuteNonQueryAsync 返回受影响行数；ON CONFLICT DO NOTHING 跳过的行不计入。
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    /// <summary>单条事件的便捷包装。</summary>
    public static Task<int> InsertAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RealtimeDatabaseSchema schema,
        RealtimeEvent evt,
        CancellationToken ct) => InsertManyAsync(connection, transaction, schema, [evt], ct);
}

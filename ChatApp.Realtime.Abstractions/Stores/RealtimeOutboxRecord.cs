using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Outbox 认领记录。四-1/五：投递目标（<see cref="TargetUserId"/>、<see cref="TargetUserIds"/>、
/// <see cref="AudienceKind"/>、<see cref="ConversationId"/>、<see cref="ExcludeUserId"/>）
/// 以数据库列为唯一权威，不再从业务 payload 反序列化。Publisher 直接发送 <see cref="PayloadUtf8"/> 字节。
/// </summary>
/// <param name="EventId">事件的幂等标识。</param>
/// <param name="EventType">实时事件类型。</param>
/// <param name="TargetUserId">单目标路由时的用户 ID。</param>
/// <param name="TargetUserIds">多目标路由时的用户 ID 集合。</param>
/// <param name="AudienceKind">受众类型。</param>
/// <param name="ConversationId">会话受众的会话 ID。</param>
/// <param name="ExcludeUserId">投递时排除的用户 ID。</param>
/// <param name="TraceParent">W3C traceparent 值。</param>
/// <param name="TraceState">W3C tracestate 值。</param>
/// <param name="Event"><c>null</c> 表示新记录（仅列权威）；非空表示旧记录回退路径。</param>
/// <param name="AttemptCount">已尝试投递的次数。</param>
/// <param name="LockOwner">当前认领者标识。</param>
/// <param name="ClaimToken">当前认领租约令牌。</param>
/// <param name="PayloadUtf8">已序列化的 UTF-8 事件载荷。</param>
public sealed record RealtimeOutboxRecord(
    string EventId,
    RealtimeEventType EventType,
    long TargetUserId,
    long[]? TargetUserIds,
    AudienceKind? AudienceKind,
    string? ConversationId,
    long? ExcludeUserId,
    string? TraceParent,
    string? TraceState,
    RealtimeEvent? Event,
    int AttemptCount,
    string LockOwner,
    string ClaimToken,
    ReadOnlyMemory<byte>? PayloadUtf8 = null);

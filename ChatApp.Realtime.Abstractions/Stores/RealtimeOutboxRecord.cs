using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// Outbox 认领记录。四-1/五：投递目标（<see cref="TargetUserId"/>、<see cref="TargetUserIds"/>、
/// <see cref="AudienceKind"/>、<see cref="ConversationId"/>）以数据库列为唯一权威，
/// 不再从业务 payload 反序列化。Publisher 直接发送 <see cref="PayloadUtf8"/> 字节。
/// </summary>
/// <param name="Event"><c>null</c> 表示新记录（仅列权威）；非空表示旧记录回退路径。</param>
public sealed record RealtimeOutboxRecord(
    string EventId,
    RealtimeEventType EventType,
    long TargetUserId,
    long[]? TargetUserIds,
    AudienceKind? AudienceKind,
    string? ConversationId,
    string? TraceParent,
    string? TraceState,
    RealtimeEvent? Event,
    int AttemptCount,
    string LockOwner,
    string ClaimToken,
    ReadOnlyMemory<byte>? PayloadUtf8 = null);

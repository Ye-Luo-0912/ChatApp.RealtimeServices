using ChatApp.Realtime.Abstractions.Routing;

namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 即时信令载荷（SDP/ICE），经受预算限制的临时信令路径转发给对端在线 Gateway。
/// <para>
/// 该载荷绝不进入 PostgreSQL、持久化 Outbox 或 JetStream 历史；也不通过
/// Realtime Event 事件管线投递。音频媒体由 WebRTC/STUN/TURN/SFU 承载，Realtime 不转发 UDP 音频包。
/// </para>
/// </summary>
public sealed record CallSignalEnvelope
{
    public required string SignalId { get; init; }

    public required string CallId { get; init; }

    public required long FromUserId { get; init; }

    public required long ToUserId { get; init; }

    public required CallCommandType Kind { get; init; }

    public required string Sdp { get; init; }

    public required long Revision { get; init; }

    public required long OccurredAtMs { get; init; }

    /// <summary>
    /// 目标用户需要确定其在线 Gateway 实例集合以转发。bytes 预算由调用方在转发前校验。
    /// </summary>
    public AudienceKind Audience => AudienceKind.User;
}
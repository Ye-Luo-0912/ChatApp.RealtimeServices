namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话命令处理结果。成功时可携带 <see cref="State"/> 与对端需收到的信令
/// （<see cref="SignalToForward"/>），失败时携带稳定错误码与失败分类。
/// </summary>
public sealed class CallProcessResult
{
    public required bool Succeeded { get; init; }

    public CallState State { get; init; }

    public CallEndReason EndReason { get; init; }

    public long Revision { get; init; }

    public CallErrorCode? ErrorCode { get; init; }

    public string? ErrorMessage { get; init; }

    /// <summary>是否因幂等重放返回（未发生新迁移）。</summary>
    public bool Replayed { get; init; }

    /// <summary>需要在临时信令路径转发给对端的 SDP（成功且携带 SDP 时）。</summary>
    public CallSignalEnvelope? SignalToForward { get; init; }

    public static CallProcessResult Success(
        CallState state,
        long revision,
        CallEndReason endReason = CallEndReason.None,
        bool replayed = false,
        CallSignalEnvelope? signalToForward = null)
        => new()
        {
            Succeeded = true,
            State = state,
            Revision = revision,
            EndReason = endReason,
            Replayed = replayed,
            SignalToForward = signalToForward
        };

    public static CallProcessResult Failed(CallErrorCode code, string message)
        => new()
        {
            Succeeded = false,
            ErrorCode = code,
            ErrorMessage = message
        };
}

/// <summary>
/// 通话状态快照（临时状态存储中的记录）。只保存通话控制元数据——
/// 绝不保存 SDP/ICE 载荷或音频数据。
/// </summary>
public sealed record CallStateSnapshot
{
    public required string CallId { get; init; }

    public required CallState State { get; init; }

    public required CallEndReason EndReason { get; init; }

    public required long CallerUserId { get; init; }

    public required long CalleeUserId { get; init; }

    public required long Revision { get; init; }

    /// <summary>最近一次成功迁移的命令 id（幂等键）。</summary>
    public required string LastCommandId { get; init; }

    /// <summary>最近一次成功迁移的命令类型。</summary>
    public required CallCommandType LastCommandType { get; init; }

    /// <summary>已转发的信令条数（预算计数）。</summary>
    public required int SignalCount { get; init; }

    /// <summary>创建时间（Unix 毫秒）。</summary>
    public required long CreatedAtMs { get; init; }

    /// <summary>最后更新时间（Unix 毫秒）。</summary>
    public required long UpdatedAtMs { get; init; }

    /// <summary>过期时间（Unix 毫秒）；从此时间起视为可超时终态。</summary>
    public required long ExpiresAtMs { get; init; }
}
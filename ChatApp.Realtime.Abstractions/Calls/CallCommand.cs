namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 客户端 / Server 上行的通话信令命令。以 call id + command id 幂等，
/// 以单调 revision 处理乱序，以 grant 授权。
/// <para>
/// <see cref="Sdp"/> 仅在受预算限制的临时信令路径转发，绝不进入持久化存储。
/// </para>
/// </summary>
public sealed record CallCommand
{
    /// <summary>幂等键：同一 call 内同一 command id 重复处理返回一致结果。</summary>
    public required string CommandId { get; init; }

    public required string CallId { get; init; }

    public required CallCommandType Type { get; init; }

    /// <summary>发起该命令的用户（主叫或被叫）。</summary>
    public required long ActorUserId { get; init; }

    /// <summary>发起该命令的会话。</summary>
    public required string ActorSessionId { get; init; }

    /// <summary>Server 签发的短期 call grant（授权输入）。</summary>
    public required CallGrant Grant { get; init; }

    /// <summary>单调 revision，用于乱序/过期判定。越大越新。</summary>
    public required long Revision { get; init; }

    /// <summary>可选 SDP（offer/answer）。仅 Invite/Accept/Reconnect 可携带。</summary>
    public string? Sdp { get; init; }

    /// <summary>客户端上报发生时间（仅诊断/展示）。</summary>
    public long ClientOccurredAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

/// <summary>
/// 通话命令入站信封。携带 ACK/NACK 回调与可信身份头（网关注入）。
/// </summary>
public sealed class CallCommandEnvelope
{
    public required CallCommand Command { get; init; }

    /// <summary>网关身份头中的用户编号（可信）；未注入时为 null。</summary>
    public long? TrustedUserId { get; init; }

    /// <summary>网关身份头中的会话编号（可信）；未注入时为 null。</summary>
    public string? TrustedSessionId { get; init; }

    private readonly Func<CancellationToken, ValueTask>? _ack;
    private readonly Func<TimeSpan?, CancellationToken, ValueTask>? _nak;
    private readonly Func<CallProcessResult, CancellationToken, ValueTask>? _reply;

    public CallCommandEnvelope(CallCommand command)
    {
        Command = command;
    }

    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    public CallCommandEnvelope(
        CallCommand command,
        Func<CancellationToken, ValueTask> ack,
        Func<TimeSpan?, CancellationToken, ValueTask> nak,
        long? trustedUserId = null,
        string? trustedSessionId = null)
    {
        Command = command;
        _ack = ack;
        _nak = nak;
        TrustedUserId = trustedUserId;
        TrustedSessionId = trustedSessionId;
    }

    [System.Diagnostics.CodeAnalysis.SetsRequiredMembers]
    public CallCommandEnvelope(
        CallCommand command,
        Func<CallProcessResult, CancellationToken, ValueTask> reply,
        long? trustedUserId = null,
        string? trustedSessionId = null)
    {
        Command = command;
        _reply = reply;
        TrustedUserId = trustedUserId;
        TrustedSessionId = trustedSessionId;
    }

    public ValueTask AckAsync(CancellationToken ct = default)
        => _ack is not null ? _ack(ct) : ValueTask.CompletedTask;

    public ValueTask NakAsync(TimeSpan? delay = null, CancellationToken ct = default)
        => _nak is not null ? _nak(delay, ct) : ValueTask.CompletedTask;

    /// <summary>将处理结果回给调用方（request/reply 语义）。</summary>
    public ValueTask ReplyAsync(CallProcessResult result, CancellationToken ct = default)
        => _reply is not null ? _reply(result, ct) : ValueTask.CompletedTask;

    /// <summary>是否携带 reply 回调（request/reply 语义）。</summary>
    public bool HasReply => _reply is not null;
}
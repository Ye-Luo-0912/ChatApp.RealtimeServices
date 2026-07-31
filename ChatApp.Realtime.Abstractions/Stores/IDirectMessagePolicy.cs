namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 二-3：陌生人/好友 DM 策略接口。
/// <para>
/// 用于校验发送方是否有权向接收方发送单聊消息。策略可能包括：
/// <list type="bullet">
/// <item>允许所有人（默认）。</item>
/// <item>仅允许好友。</item>
/// <item>允许好友 + 陌生人（但限制频率）。</item>
/// </list>
/// </para>
/// <para>
/// 默认 Noop 实现返回"允许"（不阻塞），待外部系统接入时替换。
/// </para>
/// </summary>
public interface IDirectMessagePolicy
{
    /// <summary>
    /// 校验发送方是否可以向接收方发送单聊消息。
    /// </summary>
    /// <param name="senderUserId">发送方用户编号。</param>
    /// <param name="receiverUserId">接收方用户编号。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>策略校验结果。</returns>
    Task<DirectMessagePolicyResult> CheckAsync(
        long senderUserId,
        long receiverUserId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 二-3：DM 策略校验结果。
/// </summary>
public sealed class DirectMessagePolicyResult
{
    public required bool Allowed { get; init; }

    /// <summary>拒绝时的错误码（如 not_friend、blocked_by_policy）。</summary>
    public string? ErrorCode { get; init; }

    /// <summary>拒绝时的错误消息。</summary>
    public string? ErrorMessage { get; init; }
}
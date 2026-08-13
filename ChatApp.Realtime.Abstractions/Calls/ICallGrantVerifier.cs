namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 校验 Server 签发的短期 call grant。fail-closed：不可用或校验失败即拒绝。
/// </summary>
public interface ICallGrantVerifier
{
    /// <summary>
    /// 校验 grant 是否有效（签名/指纹、参与方、过期）。
    /// </summary>
    Task<CallGrantVerification> VerifyAsync(CallGrant grant, long nowMs, CancellationToken ct = default);
}

/// <summary>
/// grant 校验结果。
/// </summary>
public readonly record struct CallGrantVerification(bool Valid, CallErrorCode? Error)
{
    public static readonly CallGrantVerification Ok = new(true, null);
}
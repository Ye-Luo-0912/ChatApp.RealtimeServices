using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 通话命令失败异常（内部用于区分需要重试的瞬时冲突）。
/// </summary>
public sealed class CallConflictException : Exception
{
    public CallConflictException()
        : base("通话状态迁移发生并发冲突，需重试。")
    {
    }
}

/// <summary>
/// 默认 grant 校验器：校验结构完整、参与方有效、未过期。
/// 生产可注册 Server 签名校验器覆盖。
/// </summary>
public sealed class DefaultCallGrantVerifier : ICallGrantVerifier
{
    private readonly CallPolicyOptions _options;

    public DefaultCallGrantVerifier(CallPolicyOptions options)
    {
        _options = options;
    }

    public Task<CallGrantVerification> VerifyAsync(CallGrant grant, long nowMs, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        if (string.IsNullOrWhiteSpace(grant.CallId)
            || grant.CallerUserId <= 0
            || grant.CalleeUserId <= 0
            || grant.CallerUserId == grant.CalleeUserId)
        {
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantInvalid));
        }

        if (grant.ExpiresAtMs <= 0)
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantInvalid));

        if (grant.ExpiresAtMs + _options.GrantExpiryGraceMs < nowMs)
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantExpired));

        return Task.FromResult(CallGrantVerification.Ok);
    }
}
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// Server 签发 call grant 的 HMAC-SHA256 签名校验器（生产覆盖默认结构校验）。
/// <para>
/// 校验覆盖 Server 同款规范载荷 <c>CallId|CallerUserId|CalleeUserId|ExpiresAtMs|Nonce</c>，
/// 以共享密钥做 HMAC-SHA256 比对。fail-closed：未配置密钥、缺失签名或比对失败均拒绝；
/// 结构无效或已过期同样拒绝。签名端 <c>CallGrantSigner</c> 与本研究处使用同一 canonical 载荷。
/// </para>
/// </summary>
public sealed class SignedCallGrantVerifier : ICallGrantVerifier
{
    /// <summary>规范载荷字段分隔符，与 Server 签发端一致。</summary>
    internal const char Separator = '|';

    private readonly CallPolicyOptions _options;
    private readonly string? _secret;

    public SignedCallGrantVerifier(CallPolicyOptions options, string? secret)
    {
        _options = options;
        _secret = string.IsNullOrWhiteSpace(secret) ? null : secret;
    }

    public Task<CallGrantVerification> VerifyAsync(CallGrant grant, long nowMs, CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();

        // fail-closed：未配置签名密钥时拒绝，不猜测、不静默放行。
        if (_secret is null)
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantInvalid));

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

        // 缺失签名 → 拒绝（Server 签发的 grant 必须带签名）。
        if (string.IsNullOrWhiteSpace(grant.Signature))
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantInvalid));

        var expected = ComputeSignature(grant);
        if (!FixedTimeEquals(expected, grant.Signature))
            return Task.FromResult(new CallGrantVerification(false, CallErrorCode.GrantInvalid));

        return Task.FromResult(CallGrantVerification.Ok);
    }

    /// <summary>构造与 Server 签发端完全一致的规范载荷。</summary>
    internal static string BuildCanonicalPayload(CallGrant grant)
        => string.Concat(
            grant.CallId, Separator,
            grant.CallerUserId.ToString(CultureInfo.InvariantCulture), Separator,
            grant.CalleeUserId.ToString(CultureInfo.InvariantCulture), Separator,
            grant.ExpiresAtMs.ToString(CultureInfo.InvariantCulture), Separator,
            grant.Nonce);

    private string ComputeSignature(CallGrant grant)
    {
        var payload = BuildCanonicalPayload(grant);
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(_secret!));
        var digest = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
        return Convert.ToBase64String(digest);
    }

    private static bool FixedTimeEquals(string a, string b)
    {
        if (a.Length != b.Length)
            return false;
        var diff = 0;
        for (var i = 0; i < a.Length; i++)
            diff |= a[i] ^ b[i];
        return diff == 0;
    }
}
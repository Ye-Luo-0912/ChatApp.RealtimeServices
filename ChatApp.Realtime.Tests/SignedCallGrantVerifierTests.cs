using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Infrastructure.Core.Calls;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// CALL-CTRL-1：SignedCallGrantVerifier（HMAC-SHA256 生产校验）行为测试。
/// 覆盖签名比对、篡改拒绝、缺失签名/密钥 fail-closed、过期与结构校验。
/// </summary>
public sealed class SignedCallGrantVerifierTests
{
    private const string Secret = "call-grant-signing-test-secret-32bytes";
    private const long Caller = 1001;
    private const long Callee = 1002;
    private const long NowMs = 1_700_000_000_000;

    private readonly CallPolicyOptions _options = new();
    private readonly SignedCallGrantVerifier _verifier = new(new CallPolicyOptions(), Secret);

    [Fact]
    public async Task Valid_Signature_Passes()
    {
        var grant = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000);
        var result = await _verifier.VerifyAsync(grant, NowMs);
        Assert.True(result.Valid);
    }

    [Fact]
    public async Task Tampered_Field_Fails()
    {
        var grant = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000);
        // 篡改被叫 id，签名不再匹配。
        var tampered = grant with { CalleeUserId = Callee + 1 };
        var result = await _verifier.VerifyAsync(tampered, NowMs);
        Assert.False(result.Valid);
        Assert.Equal(CallErrorCode.GrantInvalid, result.Error);
    }

    [Fact]
    public async Task Missing_Signature_FailsClosed()
    {
        var grant = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000) with
        {
            Signature = null
        };
        var result = await _verifier.VerifyAsync(grant, NowMs);
        Assert.False(result.Valid);
        Assert.Equal(CallErrorCode.GrantInvalid, result.Error);
    }

    [Fact]
    public async Task No_Secret_FailsClosed()
    {
        var noSecret = new SignedCallGrantVerifier(new CallPolicyOptions(), secret: null);
        var grant = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000);
        var result = await noSecret.VerifyAsync(grant, NowMs);
        Assert.False(result.Valid);
        Assert.Equal(CallErrorCode.GrantInvalid, result.Error);
    }

    [Fact]
    public async Task Expired_Fails()
    {
        // 超出 GrantExpiryGraceMs（默认 500ms）宽限窗口才算过期。
        var expiredBeyondGrace = _options.GrantExpiryGraceMs + 1;
        var grant = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs - expiredBeyondGrace);
        var result = await _verifier.VerifyAsync(grant, NowMs);
        Assert.False(result.Valid);
        Assert.Equal(CallErrorCode.GrantExpired, result.Error);
    }

    [Fact]
    public async Task Invalid_Structure_Fails()
    {
        var grant = SignedGrant(caller: Caller, callee: Caller, expiresAt: NowMs + 60_000);
        var result = await _verifier.VerifyAsync(grant, NowMs);
        Assert.False(result.Valid);
        Assert.Equal(CallErrorCode.GrantInvalid, result.Error);
    }

    [Fact]
    public async Task Signature_Is_Stable_Across_Identical_Grants()
    {
        var a = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000);
        var b = SignedGrant(caller: Caller, callee: Callee, expiresAt: NowMs + 60_000);
        Assert.Equal(a.Signature, b.Signature);
    }

    /// <summary>用与 Server 同一 canonical 载荷 + 同一密钥构造 gm 签名。</summary>
    private static CallGrant SignedGrant(long caller, long callee, long expiresAt)
    {
        var grant = new CallGrant
        {
            CallId = "call-abc",
            CallerUserId = caller,
            CalleeUserId = callee,
            ExpiresAtMs = expiresAt,
            Nonce = "nonce-test",
            Signature = null
        };
        var payload = SignedCallGrantVerifier.BuildCanonicalPayload(grant);
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(Secret));
        var digest = hmac.ComputeHash(Encoding.UTF8.GetBytes(payload));
        return grant with { Signature = Convert.ToBase64String(digest) };
    }
}
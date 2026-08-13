namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// Server 签发的短期 call grant，作为 Realtime 通话信令状态的授权输入。
/// <para>
/// Realtime 不反向写 Server 业务表，只校验 grant 的有效性（参与方、过期、签名/指纹）。
/// <see cref="ICallGrantVerifier"/> 负责校验；生产实现应验证 Server 签名或指纹。
/// </para>
/// </summary>
public sealed record CallGrant
{
    public required string CallId { get; init; }

    public required long CallerUserId { get; init; }

    public required long CalleeUserId { get; init; }

    /// <summary>grant 过期时间（Unix 毫秒）。短生命周期，授权输入有界。</summary>
    public required long ExpiresAtMs { get; init; }

    /// <summary>一次性随机数，防重放。</summary>
    public required string Nonce { get; init; }

    /// <summary>不透明签名/指纹，由 <see cref="ICallGrantVerifier"/> 校验。</summary>
    public string? Signature { get; init; }
}
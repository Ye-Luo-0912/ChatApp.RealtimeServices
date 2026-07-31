namespace ChatApp.Realtime.Integration.Push;

/// <summary>
/// 单个推送投递结果（按 token 粒度）。
/// </summary>
public readonly record struct PushDeliveryOutcome
{
    /// <summary>目标令牌字符串（脱敏后用于日志/指标，不含完整 token）。</summary>
    public string TokenFingerprint { get; init; }

    /// <summary>平台（1=Fcm, 2=Apns, 3=WebPush）。</summary>
    public byte Platform { get; init; }

    /// <summary>是否投递成功（Provider 接受）。</summary>
    public bool Succeeded { get; init; }

    /// <summary>
    /// 失败原因代码（成功时为 null）：
    /// <list type="bullet">
    /// <item><c>invalid_token</c>：令牌无效，调用方应注销该 token。</item>
    /// <item><c>provider_unavailable</c>：Provider 暂时不可用，可重试。</item>
    /// <item><c>rate_limited</c>：触发 Provider 限流，可重试（带 RetryAfter）。</item>
    /// <item><c>payload_too_large</c>：负载超限，不可重试。</item>
    /// <item><c>unknown</c>：未知错误。</item>
    /// </list>
    /// </summary>
    public string? ErrorCode { get; init; }

    /// <summary>失败时的重试建议间隔（仅 <c>rate_limited</c> / <c>provider_unavailable</c> 有值）。</summary>
    public TimeSpan? RetryAfter { get; init; }
}

/// <summary>
/// 批量推送投递结果汇总。
/// </summary>
public sealed class PushDeliveryResult
{
    /// <summary>目标用户 Id。</summary>
    public required long TargetUserId { get; init; }

    /// <summary>该用户被尝试投递的令牌数（可能为 0：用户无注册令牌）。</summary>
    public int AttemptedCount { get; init; }

    /// <summary>成功投递的令牌数。</summary>
    public int SucceededCount { get; init; }

    /// <summary>应被注销的无效令牌指纹列表（调用方据此调用 IPushTokenStore.UnregisterByTokenAsync）。</summary>
    public IReadOnlyList<string> InvalidTokenFingerprints { get; init; } = Array.Empty<string>();

    /// <summary>可重试的失败数（provider_unavailable / rate_limited）。</summary>
    public int RetryableFailureCount { get; init; }

    /// <summary>用户无注册令牌时为 true（调用方可记录跳过指标）。</summary>
    public bool NoTokensRegistered => AttemptedCount == 0;

    public static PushDeliveryResult Skipped(long targetUserId) =>
        new() { TargetUserId = targetUserId };

    public static PushDeliveryResult FromOutcomes(
        long targetUserId,
        IReadOnlyList<PushDeliveryOutcome> outcomes)
    {
        var succeeded = 0;
        var retryable = 0;
        var invalid = new List<string>();
        foreach (var o in outcomes)
        {
            if (o.Succeeded)
            {
                succeeded++;
            }
            else if (o.ErrorCode == "invalid_token")
            {
                invalid.Add(o.TokenFingerprint);
            }
            else if (o.ErrorCode is "provider_unavailable" or "rate_limited")
            {
                retryable++;
            }
        }

        return new PushDeliveryResult
        {
            TargetUserId = targetUserId,
            AttemptedCount = outcomes.Count,
            SucceededCount = succeeded,
            InvalidTokenFingerprints = invalid,
            RetryableFailureCount = retryable
        };
    }
}

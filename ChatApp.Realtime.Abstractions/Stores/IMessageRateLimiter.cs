namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 二-5：消息发送频率限制接口。
/// <para>
/// 用于防垃圾和限流。支持按用户维度的滑动窗口或令牌桶限流。
/// 默认 Noop 实现不限流（不阻塞），待外部系统接入时替换。
/// </para>
/// </summary>
public interface IMessageRateLimiter
{
    /// <summary>
    /// 尝试获取发送配额。
    /// </summary>
    /// <param name="userId">用户编号。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>限流结果。</returns>
    Task<RateLimitResult> TryAcquireAsync(
        long userId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 二-5：限流结果。
/// </summary>
public sealed class RateLimitResult
{
    public required bool Allowed { get; init; }

    /// <summary>拒绝时的建议重试间隔（毫秒）。</summary>
    public int? RetryAfterMs { get; init; }

    /// <summary>拒绝时的错误码（如 rate_limited）。</summary>
    public string? ErrorCode { get; init; }
}
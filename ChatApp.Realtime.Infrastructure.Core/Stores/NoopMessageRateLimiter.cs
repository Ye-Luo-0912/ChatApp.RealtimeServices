using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Stores;

/// <summary>
/// 二-5：空实现，始终不限流。
/// </summary>
public sealed class NoopMessageRateLimiter : IMessageRateLimiter
{
    public static NoopMessageRateLimiter Instance { get; } = new();

    private NoopMessageRateLimiter() { }

    public Task<RateLimitResult> TryAcquireAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(new RateLimitResult { Allowed = true });
    }
}
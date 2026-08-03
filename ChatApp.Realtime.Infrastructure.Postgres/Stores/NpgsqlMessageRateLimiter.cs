using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// 二-5：基于内存滑动窗口的消息发送频率限制实现。
/// <para>
/// 使用 <see cref="ConcurrentDictionary"/> 按用户维度维护滑动窗口，
/// 窗口大小和配额由构造参数配置（默认：30 秒窗口，最多 30 条消息）。
/// </para>
/// <para>
/// 单实例生效，多实例部署时各实例独立计数（不精确但可接受初始版本）。
/// 查询故障时返回 Allowed=true（fail-open），由调用方决定是否放行。
/// </para>
/// <para>
/// 生产环境建议替换为 Redis 分布式滑动窗口。
/// </para>
/// </summary>
public sealed class NpgsqlMessageRateLimiter : IMessageRateLimiter, IDisposable
{
    /// <summary>默认窗口大小（毫秒）。</summary>
    public const int DefaultWindowMs = 30_000;

    /// <summary>默认窗口内最大消息数。</summary>
    public const int DefaultMaxMessages = 30;

    private readonly ConcurrentDictionary<long, RateLimitBucket> _buckets = new();
    private readonly int _windowMs;
    private readonly int _maxMessages;
    private readonly TimeProvider _timeProvider;
    private readonly Timer _cleanupTimer;

    public NpgsqlMessageRateLimiter()
        : this(DefaultWindowMs, DefaultMaxMessages, TimeProvider.System)
    {
    }

    public NpgsqlMessageRateLimiter(int windowMs, int maxMessages, TimeProvider timeProvider)
    {
        _windowMs = windowMs > 0 ? windowMs : DefaultWindowMs;
        _maxMessages = maxMessages > 0 ? maxMessages : DefaultMaxMessages;
        _timeProvider = timeProvider;
        // 每分钟清理过期 bucket
        _cleanupTimer = new Timer(Cleanup, null, TimeSpan.FromSeconds(60), TimeSpan.FromSeconds(60));
    }

    public Task<RateLimitResult> TryAcquireAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        if (userId <= 0)
            return Task.FromResult(new RateLimitResult { Allowed = true });

        var now = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        var bucket = _buckets.GetOrAdd(userId, _ => new RateLimitBucket());

        lock (bucket)
        {
            // 清理过期时间戳
            var cutoff = now - _windowMs;
            while (bucket.Timestamps.Count > 0 && bucket.Timestamps.Peek() < cutoff)
                bucket.Timestamps.Dequeue();

            if (bucket.Timestamps.Count >= _maxMessages)
            {
                var retryAfter = _windowMs - (int)(now - bucket.Timestamps.Peek());
                return Task.FromResult(new RateLimitResult
                {
                    Allowed = false,
                    RetryAfterMs = Math.Max(1, retryAfter),
                    ErrorCode = "rate_limited"
                });
            }

            bucket.Timestamps.Enqueue(now);
        }

        return Task.FromResult(new RateLimitResult { Allowed = true });
    }

    private void Cleanup(object? state)
    {
        var now = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
        var cutoff = now - _windowMs;

        foreach (var kvp in _buckets)
        {
            var bucket = kvp.Value;
            lock (bucket)
            {
                while (bucket.Timestamps.Count > 0 && bucket.Timestamps.Peek() < cutoff)
                    bucket.Timestamps.Dequeue();

                if (bucket.Timestamps.Count == 0)
                    _buckets.TryRemove(kvp.Key, out _);
            }
        }
    }

    public void Dispose()
    {
        _cleanupTimer.Dispose();
    }

    private sealed class RateLimitBucket
    {
        public Queue<long> Timestamps { get; } = new();
    }
}

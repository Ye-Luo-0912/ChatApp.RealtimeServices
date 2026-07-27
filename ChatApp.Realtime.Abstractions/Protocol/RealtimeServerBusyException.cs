namespace ChatApp.Realtime.Abstractions.Protocol;

/// <summary>
/// 过载协议：服务端在队列已满或并发门超时时抛出此异常。
/// 客户端应等待 <see cref="RetryAfterMs"/> 后重试。
/// </summary>
public sealed class RealtimeServerBusyException : Exception
{
    public RealtimeServerBusyException(int retryAfterMs, string queueKind)
        : base($"服务繁忙 (queue_kind={queueKind})，请在 {retryAfterMs}ms 后重试。")
    {
        RetryAfterMs = retryAfterMs;
        QueueKind = queueKind;
    }

    /// <summary>建议的重试间隔（毫秒）。</summary>
    public int RetryAfterMs { get; }

    /// <summary>触发过载的队列类型标识。</summary>
    public string QueueKind { get; }
}

namespace ChatApp.Realtime.Abstractions.Events;

public interface IRealtimeEventConsumer
{
    /// <summary>
    /// 异步消费实时事件（含 ACK/NAK，供账号清理等可靠处理使用）。
    /// </summary>
    IAsyncEnumerable<RealtimeEventEnvelope> ConsumeAsync(CancellationToken ct = default);
}

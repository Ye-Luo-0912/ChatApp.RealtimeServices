namespace ChatApp.Realtime.Abstractions.Events;

public interface IRealtimeEventConsumer
{
    /// <summary>
    /// 异步消费实时事件。此方法用于从指定的来源（如消息队列）接收实时事件流。
    /// </summary>
    /// <param name="ct">用于请求取消异步操作的令牌。</param>
    /// <returns>返回一个表示实时事件序列的异步枚举。</returns>
    IAsyncEnumerable<RealtimeEvent> ConsumeAsync(CancellationToken ct = default);
}

namespace ChatApp.Realtime.Abstractions.Events;

public interface IRealtimeEventPublisher
{
    /// <summary>
    /// 将实时事件异步发布到当前配置的实时队列。
    /// </summary>
    /// <param name="evt">要发布的实时事件。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <returns>表示异步操作的任务。</returns>
    /// <remarks>
    /// 契约层不关心具体队列实现。当前基础设施默认使用 NATS，后续可以替换为 JetStream 或其他消息队列。
    /// </remarks>
    Task PublishAsync(RealtimeEvent evt, CancellationToken ct = default);
}

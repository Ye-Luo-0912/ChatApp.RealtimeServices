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

    /// <summary>
    /// 将携带 <see cref="RealtimeEvent.TargetUserIds"/> 的聚合事件发布到当前实时队列。
    /// 实现方应按目标用户批量查询在线 Gateway 集合，按实例聚合投递（每个实例 1 条 NATS 消息）。
    /// </summary>
    Task PublishToManyAsync(RealtimeEvent evt, CancellationToken ct = default);

    /// <summary>
    /// Perf-4：发布事件，使用预序列化的 UTF-8 payload，避免重新序列化。
    /// <para>
    /// 当 <paramref name="payload"/> 为空时回退到 <see cref="PublishAsync"/> 的序列化路径，
    /// 兼容 <c>payload_utf8</c> 为 NULL 的旧数据。
    /// </para>
    /// </summary>
    /// <param name="evt">原始事件（用于路由查询）。</param>
    /// <param name="payload">预序列化的 UTF-8 字节；为 null 或空时回退到序列化。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    Task PublishWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payload, CancellationToken ct = default);

    /// <summary>
    /// Perf-4：发布多目标事件，使用预序列化的 UTF-8 payload。
    /// <para>
    /// 当 <paramref name="payload"/> 为空时回退到 <see cref="PublishToManyAsync"/> 的序列化路径。
    /// </para>
    /// </summary>
    /// <param name="evt">原始事件（用于路由查询）。</param>
    /// <param name="payload">预序列化的 UTF-8 字节；为 null 或空时回退到序列化。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    Task PublishToManyWithPayloadAsync(RealtimeEvent evt, ReadOnlyMemory<byte>? payload, CancellationToken ct = default);
}

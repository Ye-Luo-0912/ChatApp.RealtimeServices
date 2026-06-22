namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 定义了入站消息消费者的契约，任何实现此接口的类都必须提供从消息源异步消费消息的能力。
/// </summary>
public interface IIncomingMessageConsumer
{
    /// <summary>
    /// 从消息源异步消费入站消息。
    /// </summary>
    /// <param name="ct">取消令牌，用于支持操作的取消。</param>
    /// <returns>返回一个可异步枚举的序列，表示接收到的入站消息命令。</returns>
    /// <remarks>
    /// 该方法会根据实现的不同，从不同的消息源（如NATS）订阅并消费消息。在没有配置真实的消息消费者时，可能仅用于调试或占位用途，并不会实际返回任何值。
    /// </remarks>
    IAsyncEnumerable<IncomingMessageEnvelope> ConsumeAsync(CancellationToken ct = default);
}

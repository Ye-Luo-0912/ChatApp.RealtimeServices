namespace ChatApp.Realtime.Abstractions.Calls;

/// <summary>
/// 通话信令命令处理器：校验 grant、驱动状态机、处理幂等/乱序/TTL 超时/重连，
/// 并保证 SDP 只经临时信令路径转发、终态唯一。
/// </summary>
public interface ICallControlProcessor
{
    Task<CallProcessResult> ProcessAsync(CallCommand command, CancellationToken ct = default);
}

/// <summary>
/// 通话信令命令消费者（从队列/传输层拉取命令）。
/// </summary>
public interface ICallControlConsumer
{
    IAsyncEnumerable<CallCommandEnvelope> ConsumeAsync(CancellationToken ct = default);
}
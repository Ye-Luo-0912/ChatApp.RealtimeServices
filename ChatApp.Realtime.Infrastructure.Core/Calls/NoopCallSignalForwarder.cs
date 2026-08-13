using ChatApp.Realtime.Abstractions.Calls;

namespace ChatApp.Realtime.Infrastructure.Core.Calls;

/// <summary>
/// 空信令转发器（默认）。始终返回 true（未接入 NATS 时视为无需转发）。
/// </summary>
public sealed class NoopCallSignalForwarder : ICallSignalForwarder
{
    public static readonly NoopCallSignalForwarder Instance = new();

    public Task<bool> ForwardAsync(CallSignalEnvelope signal, CancellationToken ct = default)
        => Task.FromResult(true);
}

/// <summary>
/// 空通话命令消费者（默认）。
/// </summary>
public sealed class NoopCallControlConsumer : ICallControlConsumer
{
    public static readonly NoopCallControlConsumer Instance = new();

    public async IAsyncEnumerable<CallCommandEnvelope> ConsumeAsync([System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct = default)
    {
        await Task.CompletedTask;
        yield break;
    }
}
using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

public sealed class NoopMessageRecallConsumer : IMessageRecallConsumer
{
    public async IAsyncEnumerable<MessageRecallEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

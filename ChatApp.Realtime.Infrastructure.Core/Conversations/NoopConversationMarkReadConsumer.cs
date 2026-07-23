using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class NoopConversationMarkReadConsumer : IConversationMarkReadConsumer
{
    public async IAsyncEnumerable<ConversationMarkReadEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

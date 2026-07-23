using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Infrastructure.Core.Conversations;

public sealed class NoopConversationListQueryConsumer : IConversationListQueryConsumer
{
    public async IAsyncEnumerable<ConversationListQueryEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

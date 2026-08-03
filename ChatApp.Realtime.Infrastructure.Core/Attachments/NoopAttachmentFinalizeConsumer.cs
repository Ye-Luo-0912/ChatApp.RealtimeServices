using System.Runtime.CompilerServices;
using ChatApp.Realtime.Abstractions.Attachments;

namespace ChatApp.Realtime.Infrastructure.Core.Attachments;

public sealed class NoopAttachmentFinalizeConsumer : IAttachmentFinalizeConsumer
{
    public async IAsyncEnumerable<AttachmentFinalizeEnvelope> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        ct.ThrowIfCancellationRequested();
        await Task.CompletedTask.ConfigureAwait(false);
        yield break;
    }
}

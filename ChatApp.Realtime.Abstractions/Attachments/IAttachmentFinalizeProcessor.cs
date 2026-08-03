namespace ChatApp.Realtime.Abstractions.Attachments;

public interface IAttachmentFinalizeProcessor
{
    Task<AttachmentFinalizeResult> ProcessAsync(
        AttachmentFinalizeCommand command,
        CancellationToken ct = default);
}

public interface IAttachmentFinalizeConsumer
{
    IAsyncEnumerable<AttachmentFinalizeEnvelope> ConsumeAsync(CancellationToken ct = default);
}

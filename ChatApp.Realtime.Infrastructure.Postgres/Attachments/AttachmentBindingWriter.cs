using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;

namespace ChatApp.Realtime.Infrastructure.Postgres.Attachments;

/// <summary>
/// 附件绑定：在当前事务中做发送前可用性校验并将可绑定附件绑定到消息，返回带稳定错误码的结果。
/// 薄封装 <see cref="AttachmentWriteCommands.BindConfirmedToMessageAsync"/>，便于在
/// 共享事务上下文中以 Writer 形式参与编排。
/// </summary>
internal sealed class AttachmentBindingWriter
{
    private readonly RealtimeWriteSession _session;

    public AttachmentBindingWriter(RealtimeWriteSession session)
    {
        _session = session;
    }

    public Task<AttachmentBindResult> BindConfirmedToMessageAsync(
        string messageId,
        string? conversationId,
        long uploaderUserId,
        IReadOnlyList<string> attachmentIds)
    {
        return AttachmentWriteCommands.BindConfirmedToMessageAsync(
            _session.Connection,
            _session.Transaction,
            _session.Schema,
            messageId,
            conversationId,
            uploaderUserId,
            attachmentIds,
            _session.CancellationToken);
    }
}
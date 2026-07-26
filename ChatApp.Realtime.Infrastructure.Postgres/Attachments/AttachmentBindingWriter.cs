using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Transactions;

namespace ChatApp.Realtime.Infrastructure.Postgres.Attachments;

/// <summary>
/// 附件绑定：在当前事务中将 Confirmed 附件绑定到消息，返回线协议所需的附件记录。
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

    public Task<IReadOnlyList<RealtimeAttachmentRecord>> BindConfirmedToMessageAsync(
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

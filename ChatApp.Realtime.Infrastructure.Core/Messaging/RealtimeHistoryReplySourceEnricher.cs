using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

/// <summary>
/// 一-3：批量填充 Reply 源消息的撤回状态。
/// <para>
/// 对历史消息中引用了已撤回消息的记录，将 reply_to_preview 替换为"消息已撤回"、
/// reply_to_sender_user_id 设为 null，实现运行时降级而非级联更新。
/// </para>
/// <para>
/// 使用模式与 <see cref="RealtimeHistoryReactionEnricher"/> 一致：批量收集 reply_to_message_id，
/// 一次查询哪些已被撤回，避免 N+1。
/// </para>
/// </summary>
public static class RealtimeHistoryReplySourceEnricher
{
    /// <summary>
    /// 批量查询历史消息中 Reply 源消息的撤回状态，对引用已撤回消息的记录做运行时降级。
    /// </summary>
    /// <param name="messageStore">消息存储接口，用于批量查询源消息撤回状态。</param>
    /// <param name="messages">待 enrich 的历史消息列表。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>enriched 后的消息列表（引用已撤回消息的记录被降级）。</returns>
    public static async Task<IReadOnlyList<RealtimeHistoryMessage>> EnrichAsync(
        IRealtimeMessageStore messageStore,
        IReadOnlyList<RealtimeHistoryMessage> messages,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(messageStore);
        ArgumentNullException.ThrowIfNull(messages);

        // 收集所有非空 reply_to_message_id（去重）。
        var replyMessageIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var msg in messages)
        {
            if (!string.IsNullOrWhiteSpace(msg.ReplyToMessageId))
                replyMessageIds.Add(msg.ReplyToMessageId);
        }

        if (replyMessageIds.Count == 0)
            return messages;

        // 批量查询哪些源消息已被撤回。
        var recalledIds = await messageStore
            .BatchGetRecalledMessageIdsAsync(replyMessageIds, cancellationToken)
            .ConfigureAwait(false);

        if (recalledIds.Count == 0)
            return messages;

        var recalledSet = new HashSet<string>(recalledIds, StringComparer.Ordinal);
        var result = new List<RealtimeHistoryMessage>(messages.Count);
        var modified = false;

        foreach (var msg in messages)
        {
            if (!string.IsNullOrWhiteSpace(msg.ReplyToMessageId)
                && recalledSet.Contains(msg.ReplyToMessageId))
            {
                // 一-3：引用了已撤回消息，运行时降级。
                result.Add(CloneWithRecalledReply(msg));
                modified = true;
            }
            else
            {
                result.Add(msg);
            }
        }

        return modified ? result : messages;
    }

    /// <summary>
    /// 克隆消息，将 reply_to_* 降级为撤回状态。
    /// <para>
    /// RealtimeHistoryMessage 是 sealed class（非 record），不能使用 with 表达式，
    /// 需手动逐字段克隆，仅替换 ReplyToPreview 与 ReplyToSenderUserId。
    /// </para>
    /// </summary>
    private static RealtimeHistoryMessage CloneWithRecalledReply(RealtimeHistoryMessage original)
    {
        return new RealtimeHistoryMessage
        {
            MessageId = original.MessageId,
            ClientMessageId = original.ClientMessageId,
            SenderUserId = original.SenderUserId,
            ReceiverUserId = original.ReceiverUserId,
            ConversationId = original.ConversationId,
            ConversationSequence = original.ConversationSequence,
            Content = original.Content,
            ReceivedAtMs = original.ReceivedAtMs,
            DeliveredAtMs = original.DeliveredAtMs,
            ReadAtMs = original.ReadAtMs,
            Attachments = original.Attachments,
            ReplyToMessageId = original.ReplyToMessageId,
            ReplyToSenderUserId = null,
            ReplyToPreview = "消息已撤回",
            ForwardedFromMessageId = original.ForwardedFromMessageId,
            ForwardedFromSenderUserId = original.ForwardedFromSenderUserId,
            ForwardedFromPreview = original.ForwardedFromPreview,
            MentionedUserIds = original.MentionedUserIds,
            MentionedRoles = original.MentionedRoles,
            RecalledAtMs = original.RecalledAtMs,
            EditVersion = original.EditVersion,
            EditedAtMs = original.EditedAtMs,
            ChangedAtMs = original.ChangedAtMs,
            Reactions = original.Reactions
        };
    }
}
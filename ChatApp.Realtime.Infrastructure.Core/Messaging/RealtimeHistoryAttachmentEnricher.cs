using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

/// <summary>按消息 Id 批量填充附件引用，避免 N+1。</summary>
public static class RealtimeHistoryAttachmentEnricher
{
    public static async Task<IReadOnlyList<RealtimeHistoryMessage>> EnrichAsync(
        IRealtimeAttachmentStore attachmentStore,
        IReadOnlyList<RealtimeHistoryMessage> messages,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(attachmentStore);
        ArgumentNullException.ThrowIfNull(messages);
        if (messages.Count == 0)
            return messages;

        var messageIds = messages
            .Select(static m => m.MessageId)
            .Where(static id => !string.IsNullOrWhiteSpace(id))
            .Distinct(StringComparer.Ordinal)
            .ToArray();
        if (messageIds.Length == 0)
            return messages;

        var records = await attachmentStore
            .ListByMessageIdsAsync(messageIds, ct)
            .ConfigureAwait(false);
        if (records.Count == 0)
            return messages;

        var byMessage = new Dictionary<string, List<AttachmentRef>>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            if (string.IsNullOrWhiteSpace(record.MessageId))
                continue;

            if (!byMessage.TryGetValue(record.MessageId, out var list))
            {
                list = [];
                byMessage[record.MessageId] = list;
            }

            list.Add(AttachmentRefMapper.FromRecord(record));
        }

        var enriched = new RealtimeHistoryMessage[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            if (!byMessage.TryGetValue(message.MessageId, out var attachments))
            {
                enriched[i] = message;
                continue;
            }

            enriched[i] = CloneWithAttachments(message, attachments);
        }

        return enriched;
    }

    private static RealtimeHistoryMessage CloneWithAttachments(
        RealtimeHistoryMessage message,
        IReadOnlyList<AttachmentRef> attachments) =>
        new()
        {
            MessageId = message.MessageId,
            ClientMessageId = message.ClientMessageId,
            SenderUserId = message.SenderUserId,
            ReceiverUserId = message.ReceiverUserId,
            ConversationId = message.ConversationId,
            Content = message.Content,
            ReceivedAtMs = message.ReceivedAtMs,
            DeliveredAtMs = message.DeliveredAtMs,
            ReadAtMs = message.ReadAtMs,
            Attachments = attachments,
            ReplyToMessageId = message.ReplyToMessageId,
            ReplyToSenderUserId = message.ReplyToSenderUserId,
            ReplyToPreview = message.ReplyToPreview,
            ForwardedFromMessageId = message.ForwardedFromMessageId,
            ForwardedFromSenderUserId = message.ForwardedFromSenderUserId,
            ForwardedFromPreview = message.ForwardedFromPreview,
            RecalledAtMs = message.RecalledAtMs,
            EditVersion = message.EditVersion,
            EditedAtMs = message.EditedAtMs,
            ChangedAtMs = message.ChangedAtMs,
            Reactions = message.Reactions
        };
}

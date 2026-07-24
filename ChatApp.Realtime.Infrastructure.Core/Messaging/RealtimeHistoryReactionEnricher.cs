using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;

namespace ChatApp.Realtime.Infrastructure.Core.Messaging;

/// <summary>按消息 Id 批量填充反应摘要，避免 N+1。</summary>
public static class RealtimeHistoryReactionEnricher
{
    public static async Task<IReadOnlyList<RealtimeHistoryMessage>> EnrichAsync(
        IRealtimeReactionStore reactionStore,
        IReadOnlyList<RealtimeHistoryMessage> messages,
        long viewerUserId,
        CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(reactionStore);
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

        var records = await reactionStore
            .ListByMessageIdsAsync(messageIds, ct)
            .ConfigureAwait(false);
        if (records.Count == 0)
            return messages;

        var byMessage = new Dictionary<string, List<MessageReactionRecord>>(StringComparer.Ordinal);
        foreach (var record in records)
        {
            if (string.IsNullOrWhiteSpace(record.MessageId))
                continue;

            if (!byMessage.TryGetValue(record.MessageId, out var list))
            {
                list = [];
                byMessage[record.MessageId] = list;
            }

            list.Add(record);
        }

        var enriched = new RealtimeHistoryMessage[messages.Count];
        for (var i = 0; i < messages.Count; i++)
        {
            var message = messages[i];
            if (!byMessage.TryGetValue(message.MessageId, out var reactions))
            {
                enriched[i] = message;
                continue;
            }

            enriched[i] = CloneWithReactions(message, BuildSummaries(reactions, viewerUserId));
        }

        return enriched;
    }

    private static IReadOnlyList<MessageReactionSummary> BuildSummaries(
        IReadOnlyList<MessageReactionRecord> reactions,
        long viewerUserId)
    {
        var map = new Dictionary<string, (int Count, bool ReactedByMe)>(StringComparer.Ordinal);
        foreach (var reaction in reactions)
        {
            if (!map.TryGetValue(reaction.Emoji, out var current))
                current = (0, false);

            map[reaction.Emoji] = (
                current.Count + 1,
                current.ReactedByMe || reaction.UserId == viewerUserId);
        }

        return map
            .OrderBy(static kv => kv.Key, StringComparer.Ordinal)
            .Select(static kv => new MessageReactionSummary
            {
                Emoji = kv.Key,
                Count = kv.Value.Count,
                ReactedByMe = kv.Value.ReactedByMe
            })
            .ToArray();
    }

    private static RealtimeHistoryMessage CloneWithReactions(
        RealtimeHistoryMessage message,
        IReadOnlyList<MessageReactionSummary> reactions) =>
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
            Attachments = message.Attachments,
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
            Reactions = reactions
        };
}

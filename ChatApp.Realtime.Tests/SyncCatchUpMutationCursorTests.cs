using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging.History;

namespace ChatApp.Realtime.Tests;

public sealed class SyncCatchUpMutationCursorTests
{
    [Fact]
    public void PackCatchUp_UsesChangedAtMsForNextCursor()
    {
        // Mirror PackCatchUp semantics: catch-up watermark advances by changed_at, not received_at.
        var items = new List<RealtimeHistoryMessage>
        {
            new()
            {
                MessageId = "old-msg",
                ClientMessageId = "c1",
                SenderUserId = 1,
                ReceiverUserId = 2,
                ConversationId = "dm:1:2",
                Content = "edited",
                ReceivedAtMs = 100,
                EditVersion = 2,
                EditedAtMs = 500,
                ChangedAtMs = 500
            },
            new()
            {
                MessageId = "new-msg",
                ClientMessageId = "c2",
                SenderUserId = 1,
                ReceiverUserId = 2,
                ConversationId = "dm:1:2",
                Content = "fresh",
                ReceivedAtMs = 300,
                EditVersion = 1,
                ChangedAtMs = 300
            }
        };

        // Ordered as mutation catch-up would return: by changed_at ASC
        items.Sort((a, b) =>
        {
            var cmp = a.ChangedAtMs.CompareTo(b.ChangedAtMs);
            return cmp != 0 ? cmp : string.CompareOrdinal(a.MessageId, b.MessageId);
        });

        var last = items[^1];
        var cursor = new MessageHistoryCursor(
            last.ChangedAtMs > 0 ? last.ChangedAtMs : last.ReceivedAtMs,
            last.MessageId);

        Assert.Equal(500, cursor.ReceivedAtMs);
        Assert.Equal("old-msg", cursor.MessageId);
        Assert.Equal("new-msg", items[0].MessageId);
        Assert.Equal("old-msg", items[1].MessageId);
    }

    [Fact]
    public void MessageEditedEventIds_IncludeVersion()
    {
        var v2 = RealtimeEventContracts.CreateMessageEditedEventId("msg-1", 2, 2);
        var v3 = RealtimeEventContracts.CreateMessageEditedEventId("msg-1", 2, 3);
        Assert.NotEqual(v2, v3);
        Assert.Equal(
            v2,
            RealtimeEventContracts.CreateMessageEditedEventId("msg-1", 2, 2));
    }

    [Fact]
    public void BusinessName_MapsMessageEdited()
    {
        Assert.Equal(
            RealtimeEventType.MessageEdited,
            RealtimeEventContracts.ToWireType(RealtimeEventContracts.MessageEdited));
    }
}

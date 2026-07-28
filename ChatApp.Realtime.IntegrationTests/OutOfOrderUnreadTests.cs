using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.IntegrationTests.Helpers;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class OutOfOrderUnreadTests
{
    private readonly RealtimePipelineFixture _fixture;

    public OutOfOrderUnreadTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task OutOfOrderIncomingMessages_StillIncrementUnread()
    {
        await using var bus = _fixture.CreateBus("ooo");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(60));

        const long sender = 9_100_000_041;
        const long receiver = 9_100_000_042;
        var conversationId = ConversationId.CreateDirect(sender, receiver);
        var newerClientId = Guid.CreateVersion7().ToString("N");
        var olderClientId = Guid.CreateVersion7().ToString("N");
        var newerId = PipelineTestIds.CreateMessageCommandId(sender, newerClientId);
        var olderId = PipelineTestIds.CreateMessageCommandId(sender, olderClientId);
        var baseMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var newerDelivery = EventWaiter.WaitForAsync(
            bus,
            newerId,
            RealtimeEventType.MessageReceived,
            timeout.Token);
        await Task.Delay(200, timeout.Token);
        await bus.PublishIncomingMessageAsync(
            new IncomingMessageCommand
            {
                CommandId = newerId,
                ClientMessageId = newerClientId,
                SenderUserId = sender,
                SenderSessionId = "e2e-ooo",
                ReceiverUserId = receiver,
                Content = "newer",
                ReceivedAtMs = baseMs + 200
            },
            timeout.Token);
        await newerDelivery;

        var olderDelivery = EventWaiter.WaitForAsync(
            bus,
            olderId,
            RealtimeEventType.MessageReceived,
            timeout.Token);
        await Task.Delay(200, timeout.Token);
        await bus.PublishIncomingMessageAsync(
            new IncomingMessageCommand
            {
                CommandId = olderId,
                ClientMessageId = olderClientId,
                SenderUserId = sender,
                SenderSessionId = "e2e-ooo",
                ReceiverUserId = receiver,
                Content = "older",
                ReceivedAtMs = baseMs + 100
            },
            timeout.Token);
        await olderDelivery;

        var list = await bus.QueryConversationListAsync(
            new ConversationListQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiver,
                Limit = 10
            },
            timeout.Token);

        Assert.True(list.Succeeded);
        var item = Assert.Single(list.Items, x => x.ConversationId == conversationId);
        // P0-5：服务端到达时间权威。客户端上报的 ReceivedAtMs 不再决定 tip 排序，
        // 后到达服务端的消息（此处为 olderId，第二条发送）拥有更晚的 ServerReceivedAtMs，成为 tip。
        Assert.Equal(olderId, item.LastMessageId);
        Assert.Equal(2, item.UnreadCount);
    }
}

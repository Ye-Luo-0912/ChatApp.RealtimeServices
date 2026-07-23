using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.IntegrationTests.Helpers;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class MessagePersistPipelineTests
{
    private readonly RealtimePipelineFixture _fixture;

    public MessagePersistPipelineTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task PublishMessage_Persists_AndSurfacesViaHistoryListAndSync()
    {
        await using var bus = _fixture.CreateBus("persist");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var clientMessageId = Guid.CreateVersion7().ToString("N");
        const long senderUserId = 9_100_000_001;
        const long receiverUserId = 9_100_000_002;
        var conversationId = ConversationId.CreateDirect(senderUserId, receiverUserId);
        var commandId = PipelineTestIds.CreateMessageCommandId(senderUserId, clientMessageId);
        var command = new IncomingMessageCommand
        {
            CommandId = commandId,
            ClientMessageId = clientMessageId,
            SenderUserId = senderUserId,
            SenderSessionId = "e2e-persist-sender",
            ReceiverUserId = receiverUserId,
            Content = $"e2e-persist-{clientMessageId}",
            ReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var deliveryTask = EventWaiter.WaitForAsync(
            bus,
            commandId,
            RealtimeEventType.MessageReceived,
            timeout.Token);

        await Task.Delay(250, timeout.Token);
        await bus.PublishIncomingMessageAsync(command, timeout.Token);
        await deliveryTask;

        var history = await bus.QueryMessageHistoryAsync(
            new MessageHistoryQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiverUserId,
                ConversationId = conversationId,
                Limit = 20
            },
            timeout.Token);
        Assert.True(history.Succeeded);
        Assert.Contains(history.Items, item => item.MessageId == commandId);

        var list = await bus.QueryConversationListAsync(
            new ConversationListQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiverUserId,
                Limit = 20
            },
            timeout.Token);
        Assert.True(list.Succeeded);
        var listed = Assert.Single(
            list.Items,
            item => item.ConversationId == conversationId);
        Assert.Equal(commandId, listed.LastMessageId);
        Assert.True(listed.UnreadCount >= 1);

        var sync = await bus.QuerySyncBootstrapAsync(
            new SyncBootstrapQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiverUserId,
                ListLimit = 20,
                HistoryLimitPerConversation = 10,
                MaxConversationsWithHistory = 5
            },
            timeout.Token);
        Assert.True(sync.Succeeded);
        Assert.Contains(sync.Conversations, item => item.ConversationId == conversationId);
    }
}

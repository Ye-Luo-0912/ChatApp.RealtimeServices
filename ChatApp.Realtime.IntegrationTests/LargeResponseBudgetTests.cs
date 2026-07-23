using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.IntegrationTests.Helpers;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class LargeResponseBudgetTests
{
    private readonly RealtimePipelineFixture _fixture;

    public LargeResponseBudgetTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task MessageHistory_PacksUnderWireBudget_WhenManyLargeMessages()
    {
        await using var bus = _fixture.CreateBus("budget");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(120));

        const long sender = 9_100_000_051;
        const long receiver = 9_100_000_052;
        var conversationId = ConversationId.CreateDirect(sender, receiver);
        var baseMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        // ~2 KiB content × 40 ≈ well over 64 KiB when serialized with metadata.
        var chunk = new string('x', 2_000);

        for (var i = 0; i < 40; i++)
        {
            var clientMessageId = Guid.CreateVersion7().ToString("N");
            var commandId = PipelineTestIds.CreateMessageCommandId(sender, clientMessageId);
            var delivery = EventWaiter.WaitForAsync(
                bus,
                commandId,
                RealtimeEventType.MessageReceived,
                timeout.Token);
            await Task.Delay(50, timeout.Token);
            await bus.PublishIncomingMessageAsync(
                new IncomingMessageCommand
                {
                    CommandId = commandId,
                    ClientMessageId = clientMessageId,
                    SenderUserId = sender,
                    SenderSessionId = "e2e-budget",
                    ReceiverUserId = receiver,
                    Content = $"{i}:{chunk}",
                    ReceivedAtMs = baseMs + i
                },
                timeout.Token);
            await delivery;
        }

        var page = await bus.QueryMessageHistoryAsync(
            new MessageHistoryQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = receiver,
                ConversationId = conversationId,
                Limit = 100
            },
            timeout.Token);

        Assert.True(page.Succeeded);
        Assert.NotEmpty(page.Items);
        Assert.True(page.HasMore);

        var json = JsonSerializer.Serialize(page);
        var bytes = Encoding.UTF8.GetByteCount(json);
        Assert.True(
            bytes <= RealtimeWireLimits.MaximumResponseBytes,
            $"History JSON was {bytes} bytes; budget is {RealtimeWireLimits.MaximumResponseBytes}.");
        Assert.True(bytes < RealtimeWireLimits.GatewayMaxPayloadBytes);
    }
}

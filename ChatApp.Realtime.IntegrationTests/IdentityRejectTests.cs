using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.IntegrationTests.Helpers;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class IdentityRejectTests
{
    private readonly RealtimePipelineFixture _fixture;

    public IdentityRejectTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task IncomingMessage_WithoutGatewayIdentity_GoesToDeadLetter_AndDoesNotPersist()
    {
        await using var bus = _fixture.CreateBus("identity");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        var clientMessageId = Guid.CreateVersion7().ToString("N");
        const long senderUserId = 9_100_000_011;
        const long receiverUserId = 9_100_000_012;
        var commandId = PipelineTestIds.CreateMessageCommandId(senderUserId, clientMessageId);
        var command = new IncomingMessageCommand
        {
            CommandId = commandId,
            ClientMessageId = clientMessageId,
            SenderUserId = senderUserId,
            SenderSessionId = "e2e-forged-sender",
            ReceiverUserId = receiverUserId,
            Content = "should-be-rejected",
            ReceivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
        };

        var deadLetterTask = RawNatsHelpers.WaitForDeadLetterAsync(
            _fixture.NatsUrl,
            commandId,
            "missing_gateway_identity",
            timeout.Token);

        await Task.Delay(250, timeout.Token);
        await RawNatsHelpers.PublishIncomingWithoutIdentityAsync(
            _fixture.NatsUrl,
            command,
            timeout.Token);

        var letter = await deadLetterTask;
        Assert.Equal("missing_gateway_identity", letter.ReasonCode);

        // Give workers a beat, then assert no MessageReceived and no history row.
        await Task.Delay(1_000, timeout.Token);
        using var shortWait = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
        shortWait.CancelAfter(TimeSpan.FromSeconds(2));
        var sawEvent = false;
        try
        {
            await foreach (var delivery in bus.ConsumeEventsAsync(shortWait.Token))
            {
                await delivery.AckAsync(timeout.Token);
                if (delivery.Event.Type == RealtimeEventType.MessageReceived
                    && delivery.Event.MessageId == commandId)
                {
                    sawEvent = true;
                    break;
                }
            }
        }
        catch (OperationCanceledException)
        {
            // expected: no matching event
        }

        Assert.False(sawEvent);
        var stored = await bus.TryGetMessageByIdAsync(receiverUserId, commandId, timeout.Token);
        Assert.Null(stored);
    }
}

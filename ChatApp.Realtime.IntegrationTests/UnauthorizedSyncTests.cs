using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.IntegrationTests.Helpers;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class UnauthorizedSyncTests
{
    private readonly RealtimePipelineFixture _fixture;

    public UnauthorizedSyncTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task SyncBootstrap_WithMismatchedTrustedUser_FailsIdentityCheck()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var page = await RawNatsHelpers.QuerySyncBootstrapWithTrustedUserAsync(
            _fixture.NatsUrl,
            new SyncBootstrapQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = 9_100_000_021,
                ListLimit = 10,
                HistoryLimitPerConversation = 5,
                MaxConversationsWithHistory = 3
            },
            trustedUserId: 9_100_000_099,
            timeout.Token);

        Assert.False(page.Succeeded);
        Assert.Equal("history_user_identity_mismatch", page.ErrorCode);
    }

    [Fact]
    public async Task SyncBootstrap_EmitsMembershipLost_ForNonMemberConversationWatermark()
    {
        await using var bus = _fixture.CreateBus("sync-authz");
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));

        const long alice = 9_100_000_031;
        const long bob = 9_100_000_032;
        const long mallory = 9_100_000_033;
        await _fixture.EnsureDirectMessageAllowedAsync(alice, bob);
        await _fixture.EnsureUsersExistAsync(mallory);
        var privateConversation = ConversationId.CreateDirect(alice, bob);
        var clientMessageId = Guid.CreateVersion7().ToString("N");
        var commandId = PipelineTestIds.CreateMessageCommandId(alice, clientMessageId);
        var receivedAtMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        var deliveryTask = EventWaiter.WaitForAsync(
            bus,
            commandId,
            RealtimeEventType.MessageReceived,
            timeout.Token);
        await Task.Delay(250, timeout.Token);
        await bus.PublishIncomingMessageAsync(
            new IncomingMessageCommand
            {
                CommandId = commandId,
                ClientMessageId = clientMessageId,
                SenderUserId = alice,
                SenderSessionId = "e2e-alice",
                ReceiverUserId = bob,
                Content = "private-to-bob",
                ReceivedAtMs = receivedAtMs
            },
            timeout.Token);
        await deliveryTask;

        // Mallory asks for catch-up on Alice/Bob conversation via forged watermark.
        var sync = await bus.QuerySyncBootstrapAsync(
            new SyncBootstrapQuery
            {
                RequestId = Guid.CreateVersion7().ToString("N"),
                UserId = mallory,
                ListLimit = 20,
                HistoryLimitPerConversation = 20,
                MaxConversationsWithHistory = 10,
                Watermarks =
                [
                    new ConversationSyncWatermark
                    {
                        ConversationId = privateConversation,
                        AfterChangedAtMs = receivedAtMs - 1,
                        AfterMessageId = "0"
                    }
                ]
            },
            timeout.Token);

        Assert.True(sync.Succeeded);
        Assert.DoesNotContain(
            sync.Conversations,
            item => item.ConversationId == privateConversation);
        Assert.DoesNotContain(
            sync.CatchUps,
            item => item.ConversationId == privateConversation);
        var reset = Assert.Single(
            sync.ResetsRequired,
            item => item.ConversationId == privateConversation);
        Assert.Equal(SyncCursorResetReason.MembershipLost, reset.Reason);
    }
}

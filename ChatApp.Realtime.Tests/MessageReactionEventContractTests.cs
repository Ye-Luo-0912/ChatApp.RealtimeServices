using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Tests;

public sealed class MessageReactionEventContractTests
{
    [Theory]
    [InlineData(RealtimeEventContracts.ReactionAdded, RealtimeEventType.ReactionAdded)]
    [InlineData(RealtimeEventContracts.ReactionRemoved, RealtimeEventType.ReactionRemoved)]
    public void BusinessName_MapsToStableWireType(string businessName, RealtimeEventType expected)
    {
        Assert.Equal(expected, RealtimeEventContracts.ToWireType(businessName));
    }

    [Fact]
    public void ReactionEventIds_IncludeOccurredAtSoTogglesAreNotDeduped()
    {
        var first = RealtimeEventContracts.CreateReactionAddedEventId(
            "msg-1",
            targetUserId: 2,
            reactorUserId: 1,
            emoji: "👍",
            occurredAtMs: 100);
        var second = RealtimeEventContracts.CreateReactionAddedEventId(
            "msg-1",
            targetUserId: 2,
            reactorUserId: 1,
            emoji: "👍",
            occurredAtMs: 200);

        Assert.Equal(64, first.Length);
        Assert.NotEqual(first, second);
        Assert.Equal(
            first,
            RealtimeEventContracts.CreateReactionAddedEventId(
                "msg-1",
                2,
                1,
                "👍",
                100));
    }
}

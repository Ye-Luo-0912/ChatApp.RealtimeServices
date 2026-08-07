using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Tests;

public sealed class MessagePartitionKeySelectorTests
{
    private const long FirstBenchmarkUserId = 9_300_000_000;
    private const int RingSize = 4_096;

    [Theory]
    [InlineData(1L, 2L)]
    [InlineData(42L, 9_300_000_000L)]
    [InlineData(9_300_000_499L, 9_300_000_500L)]
    public void DirectPair_BothDirectionsProduceTheSameKey(long firstUserId, long secondUserId)
    {
        var selector = DefaultMessagePartitionKeySelector.Instance;

        var forward = selector.GetPartitionKey(CreateCommand(firstUserId, secondUserId));
        var reverse = selector.GetPartitionKey(CreateCommand(secondUserId, firstUserId));

        Assert.Equal(forward, reverse);
    }

    [Theory]
    [InlineData(4)]
    [InlineData(8)]
    [InlineData(16)]
    public void ConsecutivePeerRing_DistributesAcrossPowerOfTwoPartitions(int partitionCount)
    {
        var selector = DefaultMessagePartitionKeySelector.Instance;
        var counts = new int[partitionCount];

        for (var index = 0; index < RingSize; index++)
        {
            var senderUserId = FirstBenchmarkUserId + index;
            var receiverUserId = index == RingSize - 1
                ? FirstBenchmarkUserId
                : senderUserId + 1;
            var key = selector.GetPartitionKey(CreateCommand(senderUserId, receiverUserId));
            counts[(int)(key & (ulong)(partitionCount - 1))]++;
        }

        var expectedPerPartition = (double)RingSize / partitionCount;
        Assert.All(
            counts,
            count => Assert.InRange(
                count,
                (int)(expectedPerPartition * 0.75),
                (int)Math.Ceiling(expectedPerPartition * 1.25)));
    }

    private static IncomingMessageCommand CreateCommand(long senderUserId, long receiverUserId) =>
        new()
        {
            CommandId = "partition-test",
            ClientMessageId = "partition-test",
            SenderUserId = senderUserId,
            SenderSessionId = "partition-test",
            ReceiverUserId = receiverUserId,
            Content = "partition-test"
        };
}

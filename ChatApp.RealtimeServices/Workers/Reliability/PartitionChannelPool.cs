using System.Threading.Channels;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 提取自 IncomingMessageWorker / MessageReceiptWorker 的分区 Channel 创建逻辑。
/// 每个 Worker 实例拥有独立的有界 Channel 数组，按总容量除以分区数分配单通道容量。
/// </summary>
internal static class PartitionChannelPool<T>
{
    public static Channel<T>[] Create(int partitionCount, int totalCapacity)
    {
        var capacity = Math.Max(1, totalCapacity / partitionCount);
        return Enumerable.Range(0, partitionCount)
            .Select(_ => Channel.CreateBounded<T>(new BoundedChannelOptions(capacity)
            {
                SingleReader = true,
                SingleWriter = true,
                FullMode = BoundedChannelFullMode.Wait,
                AllowSynchronousContinuations = false
            }))
            .ToArray();
    }
}

using ChatApp.Realtime.Abstractions.Conversations;

namespace ChatApp.Realtime.Abstractions.Messaging;

/// <summary>
/// 消息分区键选择器：决定消息进入哪个处理分区。
/// </summary>
/// <remarks>
/// Perf-1：群消息必须按 <see cref="ConversationId"/> 分区，避免同一群不同发送者落入不同分区
/// 导致会话 tip 行锁竞争、乱序到达、未读投影冲突与 Outbox 业务顺序不稳定。
/// 单聊继续使用双方用户稳定组合，确保 A→B 与 B→A 落入同一分区。
/// </remarks>
public interface IMessagePartitionKeySelector
{
    /// <summary>
    /// 计算消息的分区键（任意稳定哈希，由调用方取模得到分区号）。
    /// </summary>
    ulong GetPartitionKey(IncomingMessageCommand command);
}

/// <summary>
/// 默认实现：
/// - 单聊：min/max(sender, receiver) 组合哈希，对称且稳定。
/// - 群聊：ConversationId 字符串哈希，保证同群所有发送者落入同一分区。
/// </summary>
public sealed class DefaultMessagePartitionKeySelector : IMessagePartitionKeySelector
{
    private const ulong FirstUserSalt = 0x9E3779B97F4A7C15UL;
    private const ulong SecondUserSalt = 0xD1B54A32D192ED03UL;

    public static readonly DefaultMessagePartitionKeySelector Instance = new();

    public ulong GetPartitionKey(IncomingMessageCommand command)
    {
        // 群聊：按 ConversationId 分区，确保同群消息串行化进入同一分区。
        if (!string.IsNullOrWhiteSpace(command.ConversationId)
            && ConversationId.IsGroup(command.ConversationId))
        {
            return StableStringHash(command.ConversationId);
        }

        // 单聊：双方用户稳定组合，A→B 与 B→A 同分区。
        var first = Math.Min(command.SenderUserId, command.ReceiverUserId);
        var second = Math.Max(command.SenderUserId, command.ReceiverUserId);
        return StableDirectPairHash(first, second);
    }

    /// <summary>
    /// 对规范化后的双方用户编号做稳定的 64-bit avalanche 混合。
    /// 调用方通常以 2 的幂作为分区数，因此低位也必须具备良好分布；简单的
    /// <c>(first * 397) ^ second</c> 会让连续用户组成的 peer ring 只命中少数分区。
    /// </summary>
    private static ulong StableDirectPairHash(long first, long second)
    {
        unchecked
        {
            var firstHash = Mix64((ulong)first + FirstUserSalt);
            var secondHash = Mix64((ulong)second + SecondUserSalt);
            return Mix64(firstHash ^ secondHash);
        }
    }

    private static ulong Mix64(ulong value)
    {
        unchecked
        {
            value ^= value >> 30;
            value *= 0xBF58476D1CE4E5B9UL;
            value ^= value >> 27;
            value *= 0x94D049BB133111EBUL;
            return value ^ (value >> 31);
        }
    }

    /// <summary>
    /// FNV-1a 风格稳定字符串哈希，避免跨进程随机化（与 GetHashCode 不同）。
    /// </summary>
    private static ulong StableStringHash(string value)
    {
        var hash = 14695981039346656037UL;
        foreach (var c in value)
        {
            hash = unchecked((hash ^ (ulong)c) * 1099511628211UL);
        }
        return hash;
    }
}

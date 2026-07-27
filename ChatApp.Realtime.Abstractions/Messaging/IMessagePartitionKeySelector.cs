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
        return unchecked((ulong)first * 397UL ^ (ulong)second);
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

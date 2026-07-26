using System.Security.Cryptography;
using System.Text;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 消息相关实时业务事件幂等 Id 工厂。
/// </summary>
public static class MessageEventIdFactory
{
    public static string CreateMessageReceivedEventId(long senderUserId, string clientMessageId)
    {
        var input = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 群消息按目标用户拆分 Outbox 行时使用；纳入 target 避免冲突吞掉。
    /// </summary>
    public static string CreateMessageReceivedEventId(
        long senderUserId,
        string clientMessageId,
        long targetUserId)
    {
        var input = Encoding.UTF8.GetBytes($"{senderUserId}:{clientMessageId}:{targetUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 群聊聚合事件的幂等键：按 (senderUserId, clientMessageId, conversationId) 派生，
    /// 不再按 target 拆分。同一群消息只产生一个 EventId，避免 per-member 拆分时的冲突吞掉。
    /// </summary>
    public static string CreateGroupMessageReceivedEventId(
        long senderUserId,
        string clientMessageId,
        string conversationId)
    {
        var input = Encoding.UTF8.GetBytes($"grpmsg:{senderUserId}:{clientMessageId}:{conversationId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateSenderEchoEventId(string messageId, long senderUserId)
    {
        var input = Encoding.UTF8.GetBytes($"msgecho:{messageId}:{senderUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMessageReceiptUpdatedEventId(
        string messageId,
        long receiverUserId,
        MessageReceiptType receiptType)
    {
        var input = Encoding.UTF8.GetBytes(
            $"{messageId}:{receiverUserId}:{(byte)receiptType}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    public static string CreateMessageRecalledEventId(string messageId, long targetUserId)
    {
        var input = Encoding.UTF8.GetBytes($"msgrecall:{messageId}:{targetUserId}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 编辑事件幂等 Id。必须纳入 <paramref name="editVersion"/>，
    /// 否则连续编辑会被 Outbox 冲突吞掉。
    /// </summary>
    public static string CreateMessageEditedEventId(
        string messageId,
        long targetUserId,
        int editVersion)
    {
        var input = Encoding.UTF8.GetBytes($"msgedit:{messageId}:{targetUserId}:{editVersion}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 反应新增事件幂等 Id。纳入 reactor + emoji + occurredAt，避免重复点赞/取消再点被吞。
    /// </summary>
    public static string CreateReactionAddedEventId(
        string messageId,
        long targetUserId,
        long reactorUserId,
        string emoji,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"reactadd:{messageId}:{targetUserId}:{reactorUserId}:{emoji}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 反应移除事件幂等 Id。纳入 reactor + emoji + occurredAt。
    /// </summary>
    public static string CreateReactionRemovedEventId(
        string messageId,
        long targetUserId,
        long reactorUserId,
        string emoji,
        long occurredAtMs)
    {
        var input = Encoding.UTF8.GetBytes(
            $"reactrm:{messageId}:{targetUserId}:{reactorUserId}:{emoji}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }
}

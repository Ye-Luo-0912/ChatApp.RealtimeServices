using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 附件相关实时业务事件幂等 Id 工厂。
/// </summary>
public static class AttachmentEventIdFactory
{
    public static string CreateAttachmentBlobsPurgeEventId(
        string cleanupEventId,
        int chunkIndex)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cleanupEventId);
        var input = Encoding.UTF8.GetBytes($"attach-purge:{cleanupEventId}:{chunkIndex}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }

    /// <summary>
    /// 六-2：使用稳定哈希（SHA256）替代 <see cref="string.GetHashCode"/>，
    /// 保证跨进程/重启后同一 cursor 产生相同 EventId，维持幂等性。
    /// 取 SHA256 前 4 字节作为 int chunkIndex，再委托给 <see cref="CreateAttachmentBlobsPurgeEventId(string, int)"/>。
    /// </summary>
    public static string CreateAttachmentBlobsPurgeEventId(
        string cleanupEventId,
        string cursor)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(cleanupEventId);
        ArgumentException.ThrowIfNullOrWhiteSpace(cursor);
        var cursorHash = SHA256.HashData(Encoding.UTF8.GetBytes(cursor));
        var chunkIndex = BitConverter.ToInt32(cursorHash, 0);
        return CreateAttachmentBlobsPurgeEventId(cleanupEventId, chunkIndex);
    }
}

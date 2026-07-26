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
}

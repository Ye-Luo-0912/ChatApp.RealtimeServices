using System.Security.Cryptography;
using System.Text;

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 会话失效相关实时业务事件幂等 Id 工厂。
/// </summary>
public static class SessionEventIdFactory
{
    public static string CreateSessionRevokedEventId(
        long targetUserId,
        string sessionId,
        long occurredAtMs)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(sessionId);
        var input = Encoding.UTF8.GetBytes(
            $"sessrev:{targetUserId}:{sessionId}:{occurredAtMs}");
        return Convert.ToHexStringLower(SHA256.HashData(input));
    }
}

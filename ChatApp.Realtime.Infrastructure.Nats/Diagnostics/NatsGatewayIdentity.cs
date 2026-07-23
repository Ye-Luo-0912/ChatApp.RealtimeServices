using NATS.Client.Core;

namespace ChatApp.Realtime.Infrastructure.Nats.Diagnostics;

/// <summary>
/// 从 NATS 消息头提取网关可信身份，并校验 payload 用户字段是否匹配。
/// </summary>
public static class NatsGatewayIdentity
{
    public static (long? UserId, string? SessionId) Extract(
        NatsHeaders? headers,
        string userIdHeader,
        string sessionIdHeader)
    {
        if (headers is null)
            return (null, null);

        long? userId = null;
        if (headers.TryGetValue(userIdHeader, out var userIdValues)
            && long.TryParse(userIdValues.ToString(), out var parsed)
            && parsed > 0)
        {
            userId = parsed;
        }

        string? sessionId = null;
        if (headers.TryGetValue(sessionIdHeader, out var sessionValues))
        {
            var raw = sessionValues.ToString();
            if (!string.IsNullOrWhiteSpace(raw))
                sessionId = raw;
        }

        return (userId, sessionId);
    }

    public static string? ValidateIncomingSender(
        bool requireGatewayIdentity,
        long? trustedUserId,
        string? trustedSessionId,
        long payloadSenderUserId,
        string payloadSenderSessionId)
    {
        if (!requireGatewayIdentity)
            return null;

        if (trustedUserId is null)
            return "missing_gateway_identity";

        if (trustedUserId.Value != payloadSenderUserId)
            return "sender_identity_mismatch";

        if (!string.IsNullOrWhiteSpace(trustedSessionId)
            && !string.Equals(trustedSessionId, payloadSenderSessionId, StringComparison.Ordinal))
        {
            return "session_identity_mismatch";
        }

        return null;
    }

    public static string? ValidateHistoryUser(
        bool requireGatewayIdentity,
        long? trustedUserId,
        long payloadUserId)
    {
        if (!requireGatewayIdentity)
            return null;

        if (trustedUserId is null)
            return "missing_gateway_identity";

        if (trustedUserId.Value != payloadUserId)
            return "history_user_identity_mismatch";

        return null;
    }

    public static string? ValidateReceiptReceiver(
        bool requireGatewayIdentity,
        long? trustedUserId,
        long payloadReceiverUserId)
    {
        if (!requireGatewayIdentity)
            return null;

        if (trustedUserId is null)
            return "missing_gateway_identity";

        if (trustedUserId.Value != payloadReceiverUserId)
            return "receipt_identity_mismatch";

        return null;
    }
}

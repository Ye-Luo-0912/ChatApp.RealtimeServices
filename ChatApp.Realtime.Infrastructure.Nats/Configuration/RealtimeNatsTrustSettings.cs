using ChatApp.Realtime.Abstractions.Auth;

namespace ChatApp.Realtime.Infrastructure.Nats.Configuration;

/// <summary>
/// 已按环境解析的 NATS 信任边界设置。
/// </summary>
public sealed class RealtimeNatsTrustSettings
{
    public required bool RequireGatewayIdentity { get; init; }
    public required string UserIdHeader { get; init; }
    public required string SessionIdHeader { get; init; }

    public static RealtimeNatsTrustSettings From(
        NatsTrustOptions trust,
        bool isDevelopment)
    {
        var require = trust.RequireGatewayIdentity ?? !isDevelopment;
        return new RealtimeNatsTrustSettings
        {
            RequireGatewayIdentity = require,
            UserIdHeader = string.IsNullOrWhiteSpace(trust.UserIdHeader)
                ? RealtimeIdentityHeaders.UserId
                : trust.UserIdHeader,
            SessionIdHeader = string.IsNullOrWhiteSpace(trust.SessionIdHeader)
                ? RealtimeIdentityHeaders.SessionId
                : trust.SessionIdHeader
        };
    }
}

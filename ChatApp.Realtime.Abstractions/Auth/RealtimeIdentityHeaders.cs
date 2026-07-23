namespace ChatApp.Realtime.Abstractions.Auth;

/// <summary>
/// 网关注入到 NATS 消息头的可信身份声明。消费者应优先信任这些头，而非 payload 内的用户字段。
/// </summary>
public static class RealtimeIdentityHeaders
{
    public const string UserId = "X-Chat-User-Id";
    public const string SessionId = "X-Chat-Session-Id";
}

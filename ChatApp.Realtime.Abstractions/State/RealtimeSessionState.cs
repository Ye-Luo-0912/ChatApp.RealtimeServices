namespace ChatApp.Realtime.Abstractions.State;

/// <summary>
/// 表示实时会话状态的类。该类封装了与会话相关的必要信息，如会话标识、用户ID以及连接和设备的信息。
/// </summary>
/// <remarks>
/// 本类设计为不可变类型，其属性通过初始化器设置或在构造时确定。这有助于确保数据的一致性和安全性，在多线程环境中尤为重要。
/// </remarks>
public sealed class RealtimeSessionState
{
    public required string SessionId { get; init; }
    public required long UserId { get; init; }
    public string? ConnectionId { get; init; }
    public string? DeviceId { get; init; }
    public long UpdatedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

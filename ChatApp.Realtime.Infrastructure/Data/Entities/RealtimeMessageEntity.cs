namespace ChatApp.Realtime.Infrastructure.Data.Entities;

/// <summary>
/// 代表实时消息的实体类，用于存储和传输即时通讯中的消息数据。
/// 该类包含消息的基本信息，如消息ID、发送者用户ID、接收者用户ID、内容等。
/// </summary>
public sealed class RealtimeMessageEntity
{
    public required string MessageId { get; init; }
    public required string ClientMessageId { get; init; }
    public required long SenderUserId { get; init; }
    public required string SenderSessionId { get; init; }
    public required long ReceiverUserId { get; init; }
    public required string Content { get; init; }
    public long ReceivedAtMs { get; init; }
    public long CreatedAtMs { get; init; } = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
}

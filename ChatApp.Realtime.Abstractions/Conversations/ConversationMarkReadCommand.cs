namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 将会话已读游标推进到指定消息（或会话当前最后一条）。多设备取 max 合并，不回退。
/// </summary>
public sealed class ConversationMarkReadCommand
{
    public required string RequestId { get; init; }
    public long UserId { get; init; }
    public required string ConversationId { get; init; }

    /// <summary>
    /// 可选提示；权威已读时间由服务端按 <see cref="ReadMessageId"/> 从库解析，忽略本字段。
    /// </summary>
    public long? ReadAtMs { get; init; }

    /// <summary>
    /// 已读锚点消息；与 <see cref="ReadAtMs"/> 皆空则推进到会话当前最后消息。
    /// </summary>
    public string? ReadMessageId { get; init; }
}

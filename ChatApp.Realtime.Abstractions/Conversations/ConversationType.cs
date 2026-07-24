namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 会话类型。单聊与群聊。
/// </summary>
public enum ConversationType : byte
{
    Direct = 1,
    Group = 2
}

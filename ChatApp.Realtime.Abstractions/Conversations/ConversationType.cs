namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 会话类型。群聊预留，当前仅实现单聊。
/// </summary>
public enum ConversationType : byte
{
    Direct = 1,
    Group = 2
}

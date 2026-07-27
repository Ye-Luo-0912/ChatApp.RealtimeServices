namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>群会话变更 / 查询操作。</summary>
public enum GroupConversationOperation : byte
{
    Create = 1,
    AddMembers = 2,
    RemoveMember = 3,
    Leave = 4,
    ChangeRole = 5,
    ListMembers = 6,
    Dissolve = 7
}

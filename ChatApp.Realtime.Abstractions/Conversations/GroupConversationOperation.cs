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
    Dissolve = 7,
    /// <summary>
    /// P1-2：查询会话受众（成员用户编号 + audience_version）。
    /// 与 <see cref="ListMembers"/> 不同，本操作不要求调用者必须是活跃成员——
    /// 面向 Gateway 的会话级广播投递（AudienceKind=Conversation）需要在不持有成员身份的情况下
    /// 解析会话成员集合，用于 ConversationAudienceCache 的填充与刷新。
    /// </summary>
    QueryAudience = 8,

    /// <summary>
    /// P1-4：查询消息已读回执（仅消息发送者有权查询）。
    /// 小群返回完整 reader list；大群返回 aggregate count（已读人数 / 总人数）。
    /// </summary>
    QueryReadReceipts = 9
}
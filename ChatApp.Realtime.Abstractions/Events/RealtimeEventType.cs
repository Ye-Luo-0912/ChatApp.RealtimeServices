namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 枚举类型，表示实时事件的种类。每个枚举值对应一种特定类型的实时事件。
/// 业务别名与 EventId 规则见 <see cref="RealtimeEventContracts"/>。
/// </summary>
public enum RealtimeEventType : byte
{
    FriendRequestListChanged = 1,
    FriendListChanged = 2,
    BlockedListChanged = 3,

    /// <summary>
    /// 会话摘要/列表变更（业务名 ConversationChanged）。
    /// Payload：<c>RealtimeConversationChangedPayload</c>（PayloadVersion≥1）。
    /// </summary>
    ConversationListChanged = 4,

    /// <summary>
    /// 新消息（业务名 MessageReceived）。Payload：<c>RealtimeChatMessagePayload</c>。
    /// 目标可为接收方，或发送方其他设备回声（跳过来源 SessionId）。
    /// </summary>
    MessageReceived = 5,

    /// <summary>
    /// 登录会话撤销（业务名 SessionInvalidated）。按 SessionId 精确断开。
    /// </summary>
    SessionRevoked = 6,

    /// <summary>
    /// 送达/已读状态（业务名 MessageDelivered / MessageRead）。
    /// Payload：<c>RealtimeMessageReceiptPayload</c>，由 ReceiptType 区分；目标为原发送者。
    /// </summary>
    MessageReceiptUpdated = 7,

    UserAccountDeleted = 8,
    AccountCleanupCompleted = 9,

    /// <summary>
    /// 会话未读数变更（业务名 UnreadCountChanged）。
    /// Payload：<c>RealtimeUnreadCountChangedPayload</c>。
    /// </summary>
    UnreadCountChanged = 10,

    /// <summary>
    /// 账号删除后需由 Server GC 的附件对象键列表。
    /// Payload：<c>AttachmentBlobsPurgePayload</c>（可分片）。
    /// </summary>
    AttachmentBlobsPurge = 11,

    /// <summary>
    /// 消息撤回。Payload：<c>RealtimeMessageRecalledPayload</c>。
    /// 目标可为接收方，或发送方其他设备回声。
    /// </summary>
    MessageRecalled = 12,

    /// <summary>
    /// 消息编辑。Payload：<c>RealtimeMessageEditedPayload</c>。
    /// 目标可为接收方，或发送方其他设备回声。
    /// </summary>
    MessageEdited = 13,

    /// <summary>
    /// 消息表情反应新增。Payload：<c>RealtimeReactionAddedPayload</c>。
    /// 目标为消息参与方（接收方 / 发送方其他设备）。
    /// </summary>
    ReactionAdded = 14,

    /// <summary>
    /// 消息表情反应移除。Payload：<c>RealtimeReactionRemovedPayload</c>。
    /// 目标为消息参与方（接收方 / 发送方其他设备）。
    /// </summary>
    ReactionRemoved = 15,

    /// <summary>
    /// 群成员加入。Payload：<c>RealtimeMemberJoinedPayload</c>。
    /// </summary>
    MemberJoined = 16,

    /// <summary>
    /// 成员主动退群。Payload：<c>RealtimeMemberLeftPayload</c>。
    /// </summary>
    MemberLeft = 17,

    /// <summary>
    /// 成员被移除。Payload：<c>RealtimeMemberRemovedPayload</c>。
    /// </summary>
    MemberRemoved = 18,

    /// <summary>
    /// 成员角色变更（含转让 Owner）。Payload：<c>RealtimeRoleChangedPayload</c>。
    /// </summary>
    RoleChanged = 19,

    /// <summary>
    /// 成员会话已读水位推进（业务名 ConversationRead）。
    /// Payload：<c>RealtimeConversationReadPayload</c>。
    /// 目标为会话其他活跃成员（不含读者本人；读者通过 UnreadCountChanged 同步）。
    /// </summary>
    ConversationRead = 20,

    /// <summary>
    /// 附件生命周期变更（上传确认/扫描/可用/拒绝/过期/缩略图更新）。
    /// Payload：<c>RealtimeAttachmentLifecyclePayload</c>；目标为上传者本人。
    /// </summary>
    AttachmentLifecycleChanged = 21
}

namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 实时业务事件名常量。每个业务名对应一个 <see cref="RealtimeEventType"/> 线协议枚举值。
/// Gateway 只依赖 Abstractions DTO，不依赖 Realtime 数据库模型。
/// </summary>
public static class RealtimeEventNames
{
    /// <summary>业务名 ConversationChanged → 线协议 <see cref="RealtimeEventType.ConversationListChanged"/>。</summary>
    public const string ConversationChanged = nameof(ConversationChanged);

    /// <summary>业务名 UnreadCountChanged → 线协议 <see cref="RealtimeEventType.UnreadCountChanged"/>。</summary>
    public const string UnreadCountChanged = nameof(UnreadCountChanged);

    /// <summary>业务名 MessageReceived → 线协议 <see cref="RealtimeEventType.MessageReceived"/>。</summary>
    public const string MessageReceived = nameof(MessageReceived);

    /// <summary>
    /// 业务名 MessageDelivered / MessageRead → 线协议
    /// <see cref="RealtimeEventType.MessageReceiptUpdated"/> + <see cref="MessageReceiptType"/>。
    /// </summary>
    public const string MessageReceiptUpdated = nameof(MessageReceiptUpdated);

    /// <summary>业务名 SessionInvalidated → 线协议 <see cref="RealtimeEventType.SessionRevoked"/>。</summary>
    public const string SessionInvalidated = nameof(SessionInvalidated);

    /// <summary>业务名 MessageRecalled → 线协议 <see cref="RealtimeEventType.MessageRecalled"/>。</summary>
    public const string MessageRecalled = nameof(MessageRecalled);

    /// <summary>业务名 MessageEdited → 线协议 <see cref="RealtimeEventType.MessageEdited"/>。</summary>
    public const string MessageEdited = nameof(MessageEdited);

    /// <summary>业务名 ReactionAdded → 线协议 <see cref="RealtimeEventType.ReactionAdded"/>。</summary>
    public const string ReactionAdded = nameof(ReactionAdded);

    /// <summary>业务名 ReactionRemoved → 线协议 <see cref="RealtimeEventType.ReactionRemoved"/>。</summary>
    public const string ReactionRemoved = nameof(ReactionRemoved);

    /// <summary>业务名 MemberJoined → 线协议 <see cref="RealtimeEventType.MemberJoined"/>。</summary>
    public const string MemberJoined = nameof(MemberJoined);

    /// <summary>业务名 MemberLeft → 线协议 <see cref="RealtimeEventType.MemberLeft"/>。</summary>
    public const string MemberLeft = nameof(MemberLeft);

    /// <summary>业务名 MemberRemoved → 线协议 <see cref="RealtimeEventType.MemberRemoved"/>。</summary>
    public const string MemberRemoved = nameof(MemberRemoved);

    /// <summary>业务名 RoleChanged → 线协议 <see cref="RealtimeEventType.RoleChanged"/>。</summary>
    public const string RoleChanged = nameof(RoleChanged);

    /// <summary>业务名 MembersAdded → 线协议 <see cref="RealtimeEventType.MembersAdded"/>。</summary>
    public const string MembersAdded = nameof(MembersAdded);

    /// <summary>业务名 ConversationRead → 线协议 <see cref="RealtimeEventType.ConversationRead"/>。</summary>
    public const string ConversationRead = nameof(ConversationRead);

    /// <summary>业务名 ConversationDissolved → 线协议 <see cref="RealtimeEventType.ConversationDissolved"/>。</summary>
    public const string ConversationDissolved = "conversation_dissolved";
}

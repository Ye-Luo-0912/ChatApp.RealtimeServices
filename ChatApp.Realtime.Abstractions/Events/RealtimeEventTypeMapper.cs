namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 业务名到线协议枚举的映射。
/// </summary>
public static class RealtimeEventTypeMapper
{
    public static RealtimeEventType ToWireType(string businessName) =>
        businessName switch
        {
            RealtimeEventNames.ConversationChanged => RealtimeEventType.ConversationListChanged,
            RealtimeEventNames.UnreadCountChanged => RealtimeEventType.UnreadCountChanged,
            RealtimeEventNames.MessageReceived => RealtimeEventType.MessageReceived,
            RealtimeEventNames.MessageReceiptUpdated => RealtimeEventType.MessageReceiptUpdated,
            RealtimeEventNames.SessionInvalidated => RealtimeEventType.SessionRevoked,
            RealtimeEventNames.MessageRecalled => RealtimeEventType.MessageRecalled,
            RealtimeEventNames.MessageEdited => RealtimeEventType.MessageEdited,
            RealtimeEventNames.ReactionAdded => RealtimeEventType.ReactionAdded,
            RealtimeEventNames.ReactionRemoved => RealtimeEventType.ReactionRemoved,
            RealtimeEventNames.MemberJoined => RealtimeEventType.MemberJoined,
            RealtimeEventNames.MemberLeft => RealtimeEventType.MemberLeft,
            RealtimeEventNames.MemberRemoved => RealtimeEventType.MemberRemoved,
            RealtimeEventNames.RoleChanged => RealtimeEventType.RoleChanged,
            RealtimeEventNames.MembersAdded => RealtimeEventType.MembersAdded,
            RealtimeEventNames.ConversationRead => RealtimeEventType.ConversationRead,
            RealtimeEventNames.ConversationDissolved => RealtimeEventType.ConversationDissolved,
            _ => throw new ArgumentOutOfRangeException(nameof(businessName), businessName, null)
        };
}

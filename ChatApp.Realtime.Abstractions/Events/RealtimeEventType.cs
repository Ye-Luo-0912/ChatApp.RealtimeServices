namespace ChatApp.Realtime.Abstractions.Events;

/// <summary>
/// 枚举类型，表示实时事件的种类。每个枚举值对应一种特定类型的实时事件，用于在应用程序中区分不同的实时消息或状态变化。
/// </summary>
public enum RealtimeEventType : byte
{
    /// <summary>
    /// 表示好友请求列表发生变化的实时事件类型。当用户的好友请求状态（如收到新的好友请求、接受或拒绝好友请求）发生变化时，会触发此事件。
    /// </summary>
    FriendRequestListChanged = 1,

    /// <summary>
    /// 表示好友列表发生变化的实时事件类型。当用户的好友状态（如添加好友、删除好友）发生变化时，会触发此事件。
    /// </summary>
    FriendListChanged = 2,

    /// <summary>
    /// 表示屏蔽列表发生变化的实时事件类型。当用户的屏蔽列表状态（如新增屏蔽用户、解除屏蔽等）发生变化时，会触发此事件。
    /// </summary>
    BlockedListChanged = 3,

    /// <summary>
    /// 表示会话列表发生变化的实时事件类型。当用户的会话状态（如新增会话、删除会话或会话内容更新）发生变化时，会触发此事件。
    /// </summary>
    ConversationListChanged = 4,

    /// <summary>
    /// 表示接收到新消息的实时事件类型。当应用程序中的用户接收到一条新的即时消息时，会触发此事件。
    /// </summary>
    MessageReceived = 5,

    /// <summary>
    /// 表示会话被撤销的实时事件类型。当用户的某个会话（如登录会话）被系统撤销时，会触发此事件。这通常发生在用户从多个设备登录并选择退出其中一个设备的情况，或者因安全原因系统自动撤销了会话。
    /// </summary>
    SessionRevoked = 6
}

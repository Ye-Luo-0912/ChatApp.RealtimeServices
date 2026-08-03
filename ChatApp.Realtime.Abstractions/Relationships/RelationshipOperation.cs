namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>关系变更 / 查询操作。</summary>
public enum RelationshipOperation : byte
{
    SendFriendRequest = 1,
    AcceptFriendRequest = 2,
    DeclineFriendRequest = 3,
    RemoveFriend = 4,
    BlockUser = 5,
    UnblockUser = 6
}

/// <summary>关系列表类型。</summary>
public enum RelationshipListType : byte
{
    Friends = 1,
    FriendRequests = 2,
    BlockedUsers = 3
}

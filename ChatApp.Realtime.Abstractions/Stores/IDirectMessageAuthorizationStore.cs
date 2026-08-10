namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 单聊授权聚合查询。生产实现应在一次远程调用中返回用户存在性、屏蔽、隐私和好友策略结果，
/// 避免消息热路径上的串行 N+1 查询。
/// </summary>
public interface IDirectMessageAuthorizationStore
{
    Task<DirectMessageAuthorizationResult> AuthorizeAsync(
        long senderUserId,
        long receiverUserId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 标记消息存储会在消息写事务内完成单聊授权。处理器检测到该能力后仅执行限流，
/// 不再提前打开第二个数据库连接做相同授权，避免 TOCTOU 和每消息一次额外往返。
/// </summary>
public interface ITransactionalDirectMessageAuthorizationStore
{
    bool AuthorizesDirectMessagesTransactionally { get; }
}

/// <summary>单聊授权的标准化判定。</summary>
public enum DirectMessageAuthorizationDecision
{
    Allowed = 0,
    SenderNotFound = 1,
    ReceiverNotFound = 2,
    Blocked = 3,
    PrivacyRejected = 4,
    NotFriend = 5
}

/// <summary>
/// 无分配的单聊授权结果。错误码和用户文案由消息处理器统一映射，避免存储层耦合协议文案。
/// </summary>
public readonly record struct DirectMessageAuthorizationResult(
    DirectMessageAuthorizationDecision Decision)
{
    public bool Allowed => Decision == DirectMessageAuthorizationDecision.Allowed;

    public static DirectMessageAuthorizationResult Success { get; } =
        new(DirectMessageAuthorizationDecision.Allowed);
}

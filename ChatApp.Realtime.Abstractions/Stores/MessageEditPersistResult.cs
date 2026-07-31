namespace ChatApp.Realtime.Abstractions.Stores;

public enum MessageEditPersistStatus
{
    Applied = 1,
    Unchanged = 2,
    NotFound = 3,
    NotAllowed = 4,
    WindowExpired = 5,
    AlreadyRecalled = 6,
    RequestConflict = 7
}

public sealed record MessageEditPersistResult(
    MessageEditPersistStatus Status,
    string MessageId,
    long? ReceiverUserId = null,
    string? ConversationId = null,
    string? Content = null,
    int? EditVersion = null,
    long? EditedAtMs = null)
{
    /// <summary>
    /// 一-4：本次 Edit 新增的 @提及用户 Id 列表。
    /// <para><c>null</c> 表示本次编辑未修改 mentions，客户端应忽略 diff。</para>
    /// </summary>
    public IReadOnlyList<long>? AddedMentionedUserIds { get; init; }

    /// <summary>
    /// 一-4：本次 Edit 移除的 @提及用户 Id 列表。
    /// <para><c>null</c> 表示本次编辑未修改 mentions，客户端应忽略 diff。</para>
    /// </summary>
    public IReadOnlyList<long>? RemovedMentionedUserIds { get; init; }
}

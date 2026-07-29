namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 账号清理 Saga 的作业记录。每行对应一个用户的一个清理阶段。
/// </summary>
/// <param name="UserId">目标用户 ID。</param>
/// <param name="Phase">清理阶段：<c>attachments</c>、<c>metadata</c>、<c>completed</c>。</param>
/// <param name="Cursor">续跑游标（如最后处理的附件 object_key），<c>null</c> 表示从开头开始。</param>
/// <param name="Status">作业状态：<c>pending</c>、<c>running</c>、<c>completed</c>、<c>failed</c>。</param>
/// <param name="RetryCount">已重试次数。</param>
/// <param name="UpdatedAtMs">最近更新时间（Unix 毫秒）。</param>
/// <param name="ClaimToken">六-1：租约令牌，认领时生成；后续操作须校验此值防止旧 lease 误操作。</param>
/// <param name="LockedBy">六-1：持有租约的实例 ID。</param>
/// <param name="LockedUntilMs">六-1：租约到期时间（Unix 毫秒），过期后 running 作业可被重新认领。</param>
public sealed record AccountCleanupJob(
    long UserId,
    string Phase,
    string? Cursor,
    string Status,
    int RetryCount,
    long UpdatedAtMs,
    string? ClaimToken = null,
    string? LockedBy = null,
    long? LockedUntilMs = null)
{
    public const string PhaseAttachments = "attachments";
    public const string PhaseMetadata = "metadata";
    public const string PhaseCompleted = "completed";

    public const string StatusPending = "pending";
    public const string StatusRunning = "running";
    public const string StatusCompleted = "completed";
    public const string StatusFailed = "failed";
}

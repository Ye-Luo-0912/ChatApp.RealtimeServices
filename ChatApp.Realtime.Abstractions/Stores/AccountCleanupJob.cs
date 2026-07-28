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
public sealed record AccountCleanupJob(
    long UserId,
    string Phase,
    string? Cursor,
    string Status,
    int RetryCount,
    long UpdatedAtMs)
{
    public const string PhaseAttachments = "attachments";
    public const string PhaseMetadata = "metadata";
    public const string PhaseCompleted = "completed";

    public const string StatusPending = "pending";
    public const string StatusRunning = "running";
    public const string StatusCompleted = "completed";
    public const string StatusFailed = "failed";
}

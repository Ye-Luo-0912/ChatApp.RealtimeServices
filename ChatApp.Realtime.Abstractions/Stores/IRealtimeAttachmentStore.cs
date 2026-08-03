using ChatApp.Realtime.Abstractions.Attachments;

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 正式附件元数据存储。Server 可经同一 Postgres 连接串写入 Confirmed；
/// Realtime 在 SaveAsync 事务内绑定，账号删除时清理行并返回 object_key。
/// </summary>
public interface IRealtimeAttachmentStore
{
    /// <summary>
    /// 写入或幂等确认已上传对象（status=Confirmed）。
    /// 若提供 <see cref="RealtimeAttachmentRecord.ClientAttachmentId"/>，
    /// 同一 uploader 重复确认返回已有行（不覆盖不同 object_key 的冲突行会抛错）。
    /// </summary>
    Task<RealtimeAttachmentRecord> InsertConfirmedAsync(
        RealtimeAttachmentRecord attachment,
        CancellationToken ct = default);

    /// <summary>
    /// 将 Confirmed 附件绑定到消息；返回成功绑定的行数。
    /// 调用方应在与消息写入同一事务中执行（见消息存储 SaveAsync）。
    /// </summary>
    Task<int> BindToMessageAsync(
        string messageId,
        string? conversationId,
        long uploaderUserId,
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default);
    /// <summary>
    /// 确认附件上传完成：Ticketed(0) → Uploaded(4)。
    /// 幂等：已是 Uploaded 返回成功；状态不符返回失败。
    /// </summary>
    Task<AttachmentFinalizePersistResult> FinalizeUploadAsync(
        long actorUserId,
        string attachmentId,
        long sizeBytes,
        string? contentHash,
        CancellationToken ct = default);

    /// <summary>
    /// 扫描开抢：Uploaded(4) → Scanning(5)。条件更新（<c>WHERE status=Uploaded AND state_version=@版本</c>），
    /// state_version 递增并返回新版本。仅当自 Uploaded 迁移成功才返回成功。
    /// </summary>
    Task<AttachmentScanTransitionResult> BeginScanAsync(
        string attachmentId,
        long expectedStateVersion,
        CancellationToken ct = default);

    /// <summary>
    /// 扫描完成：Scanning(5) → Available(7) 或 Rejected(6)。
    /// 条件更新（<c>WHERE status=Scanning AND state_version=@版本</c>），state_version 递增。
    /// 若版本不匹配（旧扫描结果覆盖新状态）返回失败，绝不覆盖新状态。
    /// </summary>
    Task<AttachmentScanTransitionResult> CompleteScanAsync(
        string attachmentId,
        long expectedStateVersion,
        AttachmentScanVerdict verdict,
        long sizeBytes,
        string? contentHash,
        string? contentType,
        string? reason,
        CancellationToken ct = default);

    /// <summary>
    /// 未绑定过期：Ticketed/Uploaded/Scanning → Expired(8)。条件更新（state_version 匹配）。
    /// 返回是否转换成功。
    /// </summary>
    Task<bool> MarkExpiredAsync(
        string attachmentId,
        long expectedStateVersion,
        CancellationToken ct = default);

    /// <summary>
    /// 列出未绑定且超过保留期的过期候选（Ticketed/Uploaded/Scanning，且 message_id 为空，
    /// 创建时间早于 cutoffMs）。用于扫尾清理。
    /// </summary>
    Task<IReadOnlyList<RealtimeAttachmentRecord>> ListExpiryCandidatesAsync(
        long cutoffMs,
        int take,
        CancellationToken ct = default);

    Task<IReadOnlyList<RealtimeAttachmentRecord>> ListByMessageIdsAsync(
        IReadOnlyList<string> messageIds,
        CancellationToken ct = default);

    /// <summary>
    /// 导出用：该用户作为上传者的全部附件（可分页）。
    /// </summary>
    Task<IReadOnlyList<RealtimeAttachmentRecord>> ListForUserExportAsync(
        long userId,
        string? afterAttachmentId,
        int take,
        CancellationToken ct = default);

    /// <summary>
    /// 列出该用户作为上传者的全部 object_key（不删除）。
    /// 账号清理应先据此写入 <c>AttachmentBlobsPurge</c> Outbox，再调用 <see cref="DeleteByUserAsync"/>。
    /// </summary>
    Task<IReadOnlyList<string>> ListObjectKeysByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default);

    /// <summary>
    /// 删除该用户作为上传者的全部附件行，返回已删除的 object_key 列表。
    /// </summary>
    Task<IReadOnlyList<string>> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default);

    /// <summary>
    /// 按 attachment_id 主键批量删除附件行，返回已删除行数。
    /// 用于账号清理 Saga 分批删除。
    /// </summary>
    Task<int> DeleteByAttachmentIdsAsync(
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default);
}

public readonly record struct AttachmentFinalizePersistResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    RealtimeAttachmentRecord? Record)
{
    public static AttachmentFinalizePersistResult Ok(RealtimeAttachmentRecord record) =>
        new(true, null, null, record);

    public static AttachmentFinalizePersistResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null);
}

/// <summary>扫描状态转换持久化结果。</summary>
public readonly record struct AttachmentScanTransitionResult(
    bool Succeeded,
    string? ErrorCode,
    string? ErrorMessage,
    RealtimeAttachmentRecord? Record)
{
    public static AttachmentScanTransitionResult Ok(RealtimeAttachmentRecord record) =>
        new(true, null, null, record);

    public static AttachmentScanTransitionResult Fail(string errorCode, string errorMessage) =>
        new(false, errorCode, errorMessage, null);
}
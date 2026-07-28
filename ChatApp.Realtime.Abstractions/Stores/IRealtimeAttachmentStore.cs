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
    /// 用于账号清理 Saga 分批删除：列出 200 条 → 写 purge Outbox → 删除这 200 条 → 更新游标。
    /// 一次 DELETE（单事务），避免 <see cref="DeleteByUserAsync"/> 的循环小事务。
    /// </summary>
    Task<int> DeleteByAttachmentIdsAsync(
        IReadOnlyList<string> attachmentIds,
        CancellationToken ct = default);
}

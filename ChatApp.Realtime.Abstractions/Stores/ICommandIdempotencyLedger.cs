namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// LongTerm-1：独立命令幂等性账本。
/// <para>
/// 解耦幂等性依据与 messages 行生命周期：消息行被 retention GC 或账号删除清理后，
/// 账本仍保留命令处理结果，防止 JetStream replay 将旧命令当作新消息重新写入。
/// 保留期不少于 JetStream MaxAge（由 IdempotencyOptions + 启动校验保证）。
/// </para>
/// <para>
/// PK = (sender_user_id, client_message_id)，与 messages 唯一索引一致。
/// </para>
/// </summary>
public interface ICommandIdempotencyLedger
{
    /// <summary>
    /// 查找已有账本条目。null 表示未处理过（或账本未启用）。
    /// </summary>
    Task<IdempotencyLedgerEntry?> FindAsync(
        long senderUserId,
        string clientMessageId,
        CancellationToken ct = default);

    /// <summary>
    /// 记录命令处理结果。幂等（PK 冲突时更新 result_kind / message_id）。
    /// </summary>
    Task RecordAsync(
        string commandId,
        long senderUserId,
        string clientMessageId,
        string contentFingerprint,
        IdempotencyLedgerResultKind kind,
        string? messageId,
        long receivedAtMs,
        CancellationToken ct = default);

    /// <summary>
    /// 清理早于 cutoff 的账本行。由 IdempotencyGCWorker 周期调用。
    /// </summary>
    Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default);
}

/// <summary>账本条目。</summary>
public sealed record IdempotencyLedgerEntry(
    long SenderUserId,
    string ClientMessageId,
    string CommandId,
    string ContentFingerprint,
    IdempotencyLedgerResultKind ResultKind,
    string? MessageId,
    long ReceivedAtMs);

/// <summary>
/// 命令处理结果分类。仅 Created / Duplicate / Conflict 会被记录到账本；
/// AttachmentBindFailed / NotAllowed 等可重试或环境相关失败不记录。
/// </summary>
public enum IdempotencyLedgerResultKind : byte
{
    /// <summary>消息已成功写入（首次处理）。</summary>
    Created = 0,

    /// <summary>相同 (sender, client_message_id) 已处理，内容指纹匹配（幂等重放）。</summary>
    Duplicate = 1,

    /// <summary>相同 (sender, client_message_id) 已处理，但内容指纹不一致（冲突）。</summary>
    Conflict = 2
}

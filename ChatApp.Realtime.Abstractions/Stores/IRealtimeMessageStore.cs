namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 实时消息存储接口，定义了保存实时消息的方法。
/// 该接口的实现类负责将实时消息记录持久化到指定的数据存储中。
/// </summary>
public interface IRealtimeMessageStore
{
    /// <summary>
    /// 将实时消息记录异步保存到数据存储中。
    /// </summary>
    /// <param name="message">要保存的实时消息记录。</param>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <param name="eventToPublish">与消息在同一事务写入 Outbox 的事件。</param>
    /// <returns>原子持久化结果。</returns>
    Task<RealtimeMessagePersistResult> SaveAsync(
        RealtimeMessageRecord message,
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);

    Task<MessageReceiptPersistResult> ApplyReceiptAsync(
        MessageReceiptRecord receipt,
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);

    /// <summary>
    /// 发送方撤回消息：清空内容、写入 recalled_at_ms，并向接收方与发送方其他会话投递 Outbox 事件。
    /// </summary>
    Task<MessageRecallPersistResult> ApplyRecallAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        long recalledAtMs,
        long maxAgeMs,
        CancellationToken ct = default);

    /// <summary>
    /// 发送方编辑消息正文（不改附件）：递增 edit_version，写入 edited_at_ms / changed_at_ms，并投递 Outbox。
    /// <para>
    /// <paramref name="mentionedUserIds"/> 与 <paramref name="mentionedRoles"/> 为 <c>null</c> 时
    /// 不修改消息已存的 mentions；非空数组（包括空数组）会替换原值并经过与新增消息一致的 MentionValidator 规范化
    /// （去重 / 排除自身 / 截断 / 群成员校验 / @all|@admin 权限校验）。
    /// </para>
    /// </summary>
    Task<MessageEditPersistResult> ApplyEditAsync(
        string requestId,
        string messageId,
        long senderUserId,
        string senderSessionId,
        string content,
        long editedAtMs,
        long maxAgeMs,
        IReadOnlyList<long>? mentionedUserIds = null,
        IReadOnlyList<string>? mentionedRoles = null,
        CancellationToken ct = default);

    /// <summary>
    /// 按批删除该用户作为发送方或接收方的全部消息，直到无剩余行为止；
    /// 并尽力清理该用户相关的 Outbox 行（按 typed 列 <c>target_user_id</c> 精确匹配，
    /// 但保留 <c>AccountCleanupCompleted</c> / <c>AttachmentBlobsPurge</c>，
    /// 避免重试抹掉待发布完成事件或 blob GC 分片）。
    /// 已清理过的用户再次调用是安全的（返回 0）。
    /// </summary>
    /// <returns>累计删除的消息行数。</returns>
    Task<long> DeleteByUserAsync(
        long userId,
        int batchSize = 1000,
        CancellationToken ct = default);

    /// <summary>
    /// 一-1：查询 Reply 源消息的权威 snapshot（sender_user_id + content 截断为 preview）。
    /// <para>
    /// 用于服务端生成 Reply snapshot，替代客户端上行的值。
    /// 仅查询同会话内、未撤回的源消息。
    /// </para>
    /// </summary>
    /// <param name="messageId">源消息编号。</param>
    /// <param name="conversationId">当前消息所属会话编号（用于校验同会话）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>
    /// 源消息存在且未撤回时返回 (senderUserId, preview)；不存在或已撤回时返回 null。
    /// </returns>
    Task<(long SenderUserId, string Preview)?> GetReplySourceAsync(
        string messageId,
        string conversationId,
        CancellationToken cancellationToken = default)
        => Task.FromResult<(long SenderUserId, string Preview)?>(null);

    /// <summary>
    /// 一-3：批量查询指定消息编号中已被撤回的消息编号集合。
    /// 用于历史消息 Reply 源消息撤回降级。
    /// </summary>
    /// <param name="messageIds">待查询的消息编号集合。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>已被撤回的消息编号列表。</returns>
    Task<IReadOnlyList<string>> BatchGetRecalledMessageIdsAsync(
        IReadOnlyCollection<string> messageIds,
        CancellationToken cancellationToken = default)
        => Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());

    /// <summary>
    /// 将事件写入事务 Outbox（<c>ON CONFLICT DO NOTHING</c> / 幂等 EventId），
    /// 供 Outbox Worker 持久发布；不直接走 best-effort 总线。
    /// </summary>
    Task EnqueueEventAsync(
        Events.RealtimeEvent eventToPublish,
        CancellationToken ct = default);

    /// <summary>
    /// P1-4：解析消息元数据（会话序列号 + 发送者），用于已读回执查询的权限校验。
    /// 返回 null 表示消息不存在或不属于该会话。
    /// </summary>
    Task<(long ConversationSequence, long SenderUserId)?> GetMessageMetaAsync(
        string messageId,
        string conversationId,
        CancellationToken cancellationToken = default);
}

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 三-1/2/3/4：消息已读回执查询接口。
/// <para>
/// 利用现有 per-member 水位模型（conversation_members.last_read_sequence）推导已读者，
/// 不需要新表。成员的 last_read_sequence >= 消息的 conversation_sequence 即视为已读。
/// </para>
/// <para>
/// 分级策略：
/// <list type="bullet">
/// <item>小群（成员数 ≤ 阈值，默认 200）：返回完整 reader list。</item>
/// <item>大群（成员数 > 阈值）：返回 aggregate count（已读人数 / 总人数）。</item>
/// </list>
/// </para>
/// </summary>
public interface IRealtimeReadReceiptStore
{
    /// <summary>
    /// 三-1/4：查询指定消息的已读者列表（小群场景）。
    /// </summary>
    /// <param name="conversationId">会话编号。</param>
    /// <param name="conversationSequence">消息的会话序列号。</param>
    /// <param name="viewerUserId">查询者用户编号（必须是消息发送者）。</param>
    /// <param name="cursor">分页游标（上一页最后一条 reader 的 user_id，null 表示第一页）。</param>
    /// <param name="pageSize">每页大小。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>已读者分页结果。</returns>
    Task<MessageReaderPage> GetReadersAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        long? cursor,
        int pageSize,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// 三-2/3：查询指定消息的已读摘要（大群返回 count，小群返回 list size）。
    /// </summary>
    /// <param name="conversationId">会话编号。</param>
    /// <param name="conversationSequence">消息的会话序列号。</param>
    /// <param name="viewerUserId">查询者用户编号（必须是消息发送者）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>已读摘要。</returns>
    Task<MessageReadSummary> GetReadSummaryAsync(
        string conversationId,
        long conversationSequence,
        long viewerUserId,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// 三-1/4：消息已读者分页结果。
/// </summary>
public sealed class MessageReaderPage
{
    public required IReadOnlyList<MessageReader> Readers { get; init; }
    public long? NextCursor { get; init; }
    public bool HasMore { get; init; }
}

/// <summary>
/// 三-1：单个已读者信息。
/// </summary>
public sealed class MessageReader
{
    public required long UserId { get; init; }
    public required long ReadAtMs { get; init; }
}

/// <summary>
/// 三-2/3：消息已读摘要。
/// </summary>
public sealed class MessageReadSummary
{
    /// <summary>已读人数。</summary>
    public required int ReadCount { get; init; }

    /// <summary>总成员人数（不含已离群成员）。</summary>
    public required int TotalMemberCount { get; init; }

    /// <summary>是否为小群（返回完整 list 而非仅 count）。</summary>
    public required bool IsSmallGroup { get; init; }
}
namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// Gateway → Realtime：批量查询会话成员"当前生效免打扰"状态
/// （离线推送过滤用，ACCOUNT-OPS-1）。
/// <para>
/// 语义：成员行 <c>is_muted = true</c> 且（<c>muted_until_ms</c> 为 null 或 &gt; UTC now）
/// 视为免打扰生效。Core NATS request/reply，零持久化。
/// </para>
/// </summary>
public sealed class ConversationMutesQuery
{
    /// <summary>请求编号（可选，用于追踪与过载响应关联）。</summary>
    public string? RequestId { get; init; }

    public required string ConversationId { get; init; }

    /// <summary>待查询的成员用户 Id 集合（调用方去重后传入）。</summary>
    public required IReadOnlyList<long> MemberUserIds { get; init; }
}

/// <summary>
/// Realtime → Gateway：会话成员免打扰批量查询响应。
/// </summary>
public sealed class ConversationMutesQueryResult
{
    public string? RequestId { get; init; }
    public required bool Succeeded { get; init; }
    public string? ErrorCode { get; init; }
    public string? ErrorMessage { get; init; }
    public int? RetryAfterMs { get; init; }
    public string? QueueKind { get; init; }

    /// <summary>
    /// 当前生效免打扰的成员用户 Id（升序）。查询失败时为空集
    /// （调用方按 fail-open 处理：不过滤、照常推送）。
    /// </summary>
    public IReadOnlyList<long> MutedUserIds { get; init; } = [];

    public static ConversationMutesQueryResult Success(
        string? requestId,
        IReadOnlyList<long> mutedUserIds) =>
        new()
        {
            RequestId = requestId,
            Succeeded = true,
            MutedUserIds = mutedUserIds
        };

    public static ConversationMutesQueryResult Failed(
        string? requestId,
        string errorCode,
        string errorMessage) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = errorCode,
            ErrorMessage = errorMessage
        };

    public static ConversationMutesQueryResult ServerBusy(
        string? requestId,
        int retryAfterMs,
        string queueKind) =>
        new()
        {
            RequestId = requestId,
            Succeeded = false,
            ErrorCode = "server_busy",
            ErrorMessage = "服务繁忙，请稍后重试。",
            RetryAfterMs = retryAfterMs,
            QueueKind = queueKind
        };
}

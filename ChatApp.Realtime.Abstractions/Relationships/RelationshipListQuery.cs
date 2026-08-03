namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// 关系列表查询（Core NATS request/reply）。
/// </summary>
public sealed class RelationshipListQuery
{
    public required string RequestId { get; init; }
    public long ActorUserId { get; init; }
    public RelationshipListType ListType { get; init; }

    /// <summary>页大小（1-200）。null 或 0 表示默认值 50。</summary>
    public int? PageSize { get; init; }

    /// <summary>分页游标（opaque）。null 表示首页。</summary>
    public string? Cursor { get; init; }

    /// <summary>上行会话 Id。</summary>
    public string? ActorSessionId { get; init; }
}

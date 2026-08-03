using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>
/// 关系列表增量同步结果：返回该列表类型的当前全量或增量列表项 + 新水位。
/// </summary>
public sealed class RelationshipCatchUp
{
    /// <summary>关系列表类型。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>当前列表项（全量或增量）。</summary>
    public IReadOnlyList<RelationshipListItem> Items { get; init; } = [];

    /// <summary>是否还有更多数据（分页）。</summary>
    public bool HasMore { get; init; }

    /// <summary>下一页游标（opaque）。null 表示无更多数据。</summary>
    public string? NextCursor { get; init; }

    /// <summary>服务端推进后的新水位。客户端应持久化此值作为下次同步的 AfterChangedAtMs。</summary>
    public long NewAfterChangedAtMs { get; init; }

    /// <summary>该列表类型是否需要客户端本地全量重置（水位无效时）。</summary>
    public bool ResetRequired { get; init; }

    /// <summary>重置原因（仅当 ResetRequired=true 时有效）。</summary>
    public string? ResetReason { get; init; }
}
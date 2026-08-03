using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>
/// 关系列表增量同步结果：返回该列表类型从 <c>afterSequence</c> 起的变更日志 + 新水位。
/// <para>
/// 客户端按 <see cref="Changes"/> 中的 <see cref="RelationshipChangeLogEntry"/> 应用本地状态：
/// Upsert 时按 <see cref="RelationshipChangeLogEntry.ResourceId"/> 写入/更新本地条目，
/// Delete 时按 resourceId 移除本地条目（tombstone）。处理完收到的条目后，客户端应把
/// <see cref="NextSequence"/> 持久化为新的本地水位。
/// </para>
/// </summary>
public sealed class RelationshipCatchUp
{
    /// <summary>关系列表类型。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>增量变更日志（Upsert / Delete 条目）。</summary>
    public IReadOnlyList<RelationshipChangeLogEntry> Changes { get; init; } = [];

    /// <summary>是否还有更多数据（分页）。</summary>
    public bool HasMore { get; init; }

    /// <summary>下一页游标（opaque）。null 表示无更多数据。</summary>
    public string? NextCursor { get; init; }

    /// <summary>客户端应持久化作为下次同步 AfterSequence 的新水位（返回条目的最大序号；无返回时保持原水位）。</summary>
    public long NextSequence { get; init; }

    /// <summary>服务端仍保留的最旧序号。若客户端 AfterSequence 低于此值，则无法增量同步，必须 ResetRequired。</summary>
    public long RetentionFloorSequence { get; init; }

    /// <summary>该列表类型是否需要客户端本地全量重建（水位超出保留范围时）。</summary>
    public bool ResetRequired { get; init; }

    /// <summary>重置原因（仅当 ResetRequired=true 时有效）。</summary>
    public string? ResetReason { get; init; }
}
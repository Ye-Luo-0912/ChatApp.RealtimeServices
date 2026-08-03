namespace ChatApp.Realtime.Abstractions.Relationships;

/// <summary>
/// 关系变更日志操作类型。
/// <para>
/// <see cref="Upsert"/> 表示资源被创建或状态变更（写 payload 反映最新状态）；
/// <see cref="Delete"/> 表示资源被删除（客户端按 <see cref="RelationshipChangeLogEntry.ResourceId"/> 移除）。
/// </para>
/// </summary>
public enum RelationshipChangeOperation : byte
{
    /// <summary>创建或状态变更（upsert）。</summary>
    Upsert = 0,

    /// <summary>删除（tombstone）。</summary>
    Delete = 1
}
using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Abstractions.Sync;

/// <summary>
/// 关系列表增量同步水位。客户端按 <see cref="ListType"/> 维度维护本地水位。
/// <para>
/// 水位语义：客户端已处理所有 occurred_at_ms &lt;= <see cref="AfterChangedAtMs"/> 的关系变更事件。
/// 下次 SyncBootstrap 时服务端返回 occurred_at_ms &gt; <see cref="AfterChangedAtMs"/> 的增量变更。
/// </para>
/// </summary>
public sealed class RelationshipSyncWatermark
{
    /// <summary>关系列表类型（Friends / FriendRequests / BlockedUsers）。</summary>
    public required RelationshipListType ListType { get; init; }

    /// <summary>客户端已处理到的变更水位（毫秒）。</summary>
    public long AfterChangedAtMs { get; init; }
}

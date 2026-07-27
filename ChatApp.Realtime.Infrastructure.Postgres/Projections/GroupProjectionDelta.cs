using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Infrastructure.Postgres.Projections;

/// <summary>
/// Perf-9：群域写路径统一投影增量协议。
/// <para>
/// 一次群操作（发消息 / 编辑 / 撤回 / 反应 / MarkRead）只产生一个 <see cref="GroupProjectionDelta"/>，
/// 由 Store 在事务内声明"广播事件"与"逐用户事件"，最后一次性写入 Outbox。
/// 消除每种群事件各自维护 fan-out 循环的重复逻辑，所有聚合规则集中在此类型。
/// </para>
/// <para>
/// <b>广播事件</b>（<see cref="AddBroadcast"/> / <see cref="AddBroadcastTo"/>）：
/// 同一 payload 投递给全体成员（或指定子集），最终只产生 1 行 Outbox，携带
/// <see cref="RealtimeEvent.TargetUserIds"/>。调用方必须使用 target-independent 的 EventId 工厂
/// （如 <see cref="MessageEventIdFactory.CreateGroupMessageRecalledEventId"/>），否则同一操作会因
/// EventId 冲突被 <c>ON CONFLICT DO NOTHING</c> 吞掉。
/// </para>
/// <para>
/// <b>逐用户事件</b>（<see cref="AddPerUser"/>）：payload 因用户不同而不同（如绝对未读数），
/// 每个目标用户产生 1 行 Outbox。<see cref="UnreadCountChanged"/> 在群发消息场景下保持逐用户，
/// 因为每个成员的绝对未读数不同；如需聚合需要演进 payload 为 delta 语义（见 Perf-9 备注）。
/// </para>
/// <para>
/// 目标 Outbox 行数（200 人群）：发消息 2 + N(未读变更用户)；编辑 1-2；撤回 1-2；反应 1；MarkRead 2。
/// </para>
/// </summary>
internal sealed class GroupProjectionDelta
{
    private readonly long[] _memberUserIds;
    private readonly List<RealtimeEvent> _events = new();

    /// <param name="conversationId">群会话编号。</param>
    /// <param name="memberUserIds">群成员用户编号列表（已按 user_id 排序，来自 <c>ListActiveMemberUserIdsAsync</c>）。</param>
    public GroupProjectionDelta(string conversationId, IReadOnlyList<long> memberUserIds)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(conversationId);
        ConversationId = conversationId;
        _memberUserIds = memberUserIds.ToArray();
    }

    public string ConversationId { get; }
    public IReadOnlyList<long> MemberUserIds => _memberUserIds;
    public int MemberCount => _memberUserIds.Length;

    /// <summary>
    /// 添加一条聚合广播事件：同一 payload 投递给全体群成员。
    /// <paramref name="template"/> 的 <see cref="RealtimeEvent.TargetUserIds"/> 会被覆盖为全体成员；
    /// 其余字段（EventId / Type / PayloadJson / ...）原样保留。
    /// </summary>
    /// <remarks>调用方必须确保 <paramref name="template"/> 使用 target-independent EventId。</remarks>
    public GroupProjectionDelta AddBroadcast(RealtimeEvent template)
    {
        _events.Add(WithTargets(template, _memberUserIds));
        return this;
    }

    /// <summary>
    /// 添加一条聚合广播事件：投递给指定的成员子集（如排除读者的其余成员）。
    /// </summary>
    public GroupProjectionDelta AddBroadcastTo(RealtimeEvent template, IReadOnlyList<long> targets)
    {
        if (targets.Count == 0)
            return this;
        _events.Add(WithTargets(template, targets.ToArray()));
        return this;
    }

    /// <summary>
    /// 添加一条逐用户事件（payload 因用户而异，如绝对未读数）。
    /// 调用方需为每个目标用户单独构造事件并逐条添加。
    /// </summary>
    public GroupProjectionDelta AddPerUser(RealtimeEvent evt)
    {
        _events.Add(evt);
        return this;
    }

    /// <summary>展开为最终写入 Outbox 的事件列表（广播事件 + 逐用户事件，按添加顺序）。</summary>
    public IReadOnlyList<RealtimeEvent> Build() => _events;

    /// <summary>当前已收集的事件总数（广播 + 逐用户）。</summary>
    public int Count => _events.Count;

    private static RealtimeEvent WithTargets(RealtimeEvent template, long[] targets) => new()
    {
        EventId = template.EventId,
        Type = template.Type,
        TargetUserId = template.TargetUserId,
        ActorUserId = template.ActorUserId,
        MessageId = template.MessageId,
        SessionId = template.SessionId,
        PayloadJson = template.PayloadJson,
        OccurredAtMs = template.OccurredAtMs,
        TraceParent = template.TraceParent,
        TraceState = template.TraceState,
        TargetUserIds = targets
    };
}

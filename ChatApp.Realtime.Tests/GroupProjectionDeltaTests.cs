using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Postgres.Projections;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// Perf-9：验证 <see cref="GroupProjectionDelta"/> 的聚合规则。
/// 群域写路径依赖该协议把 N 条 per-member 事件折叠为少量广播 + 逐用户事件。
/// </summary>
public sealed class GroupProjectionDeltaTests
{
    private const string ConversationId = "grp:00000000000000000000000000000000";

    private static readonly long[] Members = [101L, 202L, 303L];

    private static RealtimeEvent Template(
        string eventId = "evt-template",
        RealtimeEventType type = RealtimeEventType.MessageRecalled,
        long targetUserId = 101L,
        long[]? targetUserIds = null) => new()
        {
            EventId = eventId,
            Type = type,
            TargetUserId = targetUserId,
            ActorUserId = targetUserId,
            MessageId = "msg-1",
            SessionId = "sess-1",
            PayloadJson = "{}",
            OccurredAtMs = 1_700_000_000_000,
            TraceParent = null,
            TraceState = null,
            TargetUserIds = targetUserIds
        };

    [Fact]
    public void AddBroadcast_OverwritesTargetUserIds_WithAllMembers()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);

        delta.AddBroadcast(Template());

        var events = delta.Build();
        var evt = Assert.Single(events);
        Assert.Equal(Members, evt.TargetUserIds);
        // 模板的其他字段保留。
        Assert.Equal("evt-template", evt.EventId);
        Assert.Equal(RealtimeEventType.MessageRecalled, evt.Type);
    }

    [Fact]
    public void AddBroadcastTo_UsesProvidedSubset_AsTargetUserIds()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);
        var subset = new long[] { 101L, 303L };

        delta.AddBroadcastTo(Template(), subset);

        var events = delta.Build();
        var evt = Assert.Single(events);
        Assert.Equal(subset, evt.TargetUserIds);
    }

    [Fact]
    public void AddBroadcastTo_EmptySubset_IsNoOp()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);

        delta.AddBroadcastTo(Template(), Array.Empty<long>());

        Assert.Empty(delta.Build());
        Assert.Equal(0, delta.Count);
    }

    [Fact]
    public void AddPerUser_KeepsEventAsIs_WithoutTargetUserIds()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);
        var perUser = Template(targetUserId: 202L);

        delta.AddPerUser(perUser);

        var events = delta.Build();
        var evt = Assert.Single(events);
        Assert.Null(evt.TargetUserIds);
        Assert.Equal(202L, evt.TargetUserId);
    }

    [Fact]
    public void Build_ReturnsEventsInInsertionOrder()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);

        delta.AddBroadcast(Template(eventId: "broadcast-1"));
        delta.AddPerUser(Template(eventId: "per-user-1", targetUserId: 202L));
        delta.AddBroadcastTo(Template(eventId: "subset"), [303L]);
        delta.AddPerUser(Template(eventId: "per-user-2", targetUserId: 101L));

        var events = delta.Build();
        Assert.Equal(4, events.Count);
        Assert.Equal("broadcast-1", events[0].EventId);
        Assert.Equal("per-user-1", events[1].EventId);
        Assert.Equal("subset", events[2].EventId);
        Assert.Equal("per-user-2", events[3].EventId);
        Assert.Equal(4, delta.Count);
    }

    [Fact]
    public void Count_TracksNumberOfAggregatedEvents_NotMembers()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);

        Assert.Equal(0, delta.Count);

        delta.AddBroadcast(Template());
        Assert.Equal(1, delta.Count);

        delta.AddPerUser(Template(eventId: "u1", targetUserId: 101L));
        delta.AddPerUser(Template(eventId: "u2", targetUserId: 202L));
        Assert.Equal(3, delta.Count);

        // 200 人群的典型场景：1 广播 + N 逐用户未读变更。
        delta.AddBroadcast(Template(eventId: "second-broadcast"));
        Assert.Equal(4, delta.Count);
    }

    [Fact]
    public void Constructor_PreservesConversationId_AndMemberSnapshot()
    {
        var mutableMembers = new List<long>(Members) { 404L };
        var delta = new GroupProjectionDelta(ConversationId, mutableMembers);

        // 修改原始列表不影响 delta 的快照。
        mutableMembers.Add(505L);

        Assert.Equal(ConversationId, delta.ConversationId);
        Assert.Equal(4, delta.MemberCount);
        Assert.Equal([101L, 202L, 303L, 404L], delta.MemberUserIds);
    }

    [Fact]
    public void AddBroadcast_DoesNotMutateTemplate_TargetUserIds()
    {
        var delta = new GroupProjectionDelta(ConversationId, Members);
        var template = Template(targetUserIds: [999L]); // 模板自带一个无关数组

        delta.AddBroadcast(template);

        // 模板自身不应被修改。
        Assert.NotNull(template.TargetUserIds);
        Assert.Equal([999L], template.TargetUserIds);
        // delta 内的拷贝烙印全体成员。
        var evt = Assert.Single(delta.Build());
        Assert.Equal(Members, evt.TargetUserIds);
    }

    [Fact]
    public void TypicalGroupMessageScenario_ProducesTwoEvents_OneBroadcastOnePerUser()
    {
        // 模拟 NpgsqlRealtimeMessageStore.AdvanceGroupConversationAndEnqueueAsync 的典型输出：
        // 1 条 ConversationChanged 广播 + 1 条 UnreadCountChanged 逐用户。
        var delta = new GroupProjectionDelta(ConversationId, Members);

        delta.AddBroadcast(Template(
            eventId: "conv-changed-agg",
            type: RealtimeEventType.ConversationListChanged));
        delta.AddPerUser(Template(
            eventId: "unread-u202",
            type: RealtimeEventType.UnreadCountChanged,
            targetUserId: 202L));

        var events = delta.Build();
        Assert.Equal(2, events.Count);
        Assert.Equal(RealtimeEventType.ConversationListChanged, events[0].Type);
        Assert.Equal(Members, events[0].TargetUserIds);
        Assert.Equal(RealtimeEventType.UnreadCountChanged, events[1].Type);
        Assert.Null(events[1].TargetUserIds);
        Assert.Equal(202L, events[1].TargetUserId);
    }

    [Fact]
    public void Empty_Constructor_ArgumentValidation()
    {
        Assert.Throws<ArgumentException>(() => new GroupProjectionDelta("", Members));
        Assert.Throws<ArgumentException>(() => new GroupProjectionDelta("   ", Members));
        // 空成员列表是允许的（如群已解散后清理路径），但广播将无目标。
        var emptyDelta = new GroupProjectionDelta(ConversationId, Array.Empty<long>());
        emptyDelta.AddBroadcast(Template());
        var evt = Assert.Single(emptyDelta.Build());
        Assert.NotNull(evt.TargetUserIds);
        Assert.Empty(evt.TargetUserIds);
    }
}

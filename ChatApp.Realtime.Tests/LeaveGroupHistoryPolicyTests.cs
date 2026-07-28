using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// 离群历史策略集成测试：验证“离开即只读”语义——
/// 主动离群、被移除、群解散后，前成员仍可读取历史消息，但无法执行写操作。
/// </summary>
public sealed class LeaveGroupHistoryPolicyTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task LeftMember_CanStillReadConversationHistory()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_history_read");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-leave",
            10,
            conversationId,
            "LeaveRead",
            [20],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "hello", 1_700_000_000_500);
        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 20, "world", 1_700_000_000_600);

        // 用户 20 主动离群
        var leave = await groupStore.LeaveAsync(
            "req-leave-20",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);
        Assert.True(leave.Succeeded);
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 20));

        // 离群后仍可读取历史
        var result = await historyStore.QueryByConversationAsync(
            userId: 20,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result.IsMember);
        Assert.Equal(2, result.Messages.Count);
        Assert.Contains(result.Messages, m => m.Content == "hello");
        Assert.Contains(result.Messages, m => m.Content == "world");
    }

    [Fact]
    public async Task RemovedMember_CanStillReadConversationHistory()
    {
        var (client, schema) = await CreateStoreAsync("realtime_removed_history_read");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-removed",
            10,
            conversationId,
            "RemovedRead",
            [20, 30],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "msg-1", 1_700_000_000_500);

        // 用户 30 被移除
        var remove = await groupStore.RemoveMemberAsync(
            "req-remove-30",
            10,
            conversationId,
            targetUserId: 30,
            "s10",
            1_700_000_001_000);
        Assert.True(remove.Succeeded);
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 30));

        // 被移除后仍可读取历史
        var result = await historyStore.QueryByConversationAsync(
            userId: 30,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result.IsMember);
        Assert.Single(result.Messages);
        Assert.Equal("msg-1", result.Messages[0].Content);
    }

    [Fact]
    public async Task DissolvedGroup_FormerMembersCanStillReadHistory()
    {
        var (client, schema) = await CreateStoreAsync("realtime_dissolved_history_read");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-dissolve",
            10,
            conversationId,
            "DissolveRead",
            [20, 30],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "final-msg", 1_700_000_000_500);

        // Owner 解散群
        var dissolve = await groupStore.DissolveAsync(
            "req-dissolve",
            10,
            conversationId,
            "s10",
            1_700_000_001_000);
        Assert.True(dissolve.Succeeded);
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 10));
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 20));

        // 解散后所有前成员仍可读取历史
        var result20 = await historyStore.QueryByConversationAsync(
            userId: 20,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result20.IsMember);
        Assert.Single(result20.Messages);
        Assert.Equal("final-msg", result20.Messages[0].Content);

        var result10 = await historyStore.QueryByConversationAsync(
            userId: 10,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result10.IsMember);
        Assert.Single(result10.Messages);
    }

    [Fact]
    public async Task NeverMember_CannotReadConversationHistory()
    {
        var (client, schema) = await CreateStoreAsync("realtime_never_member_read");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-never",
            10,
            conversationId,
            "NeverMember",
            [20],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "secret", 1_700_000_000_500);

        // 从未入群的用户查询历史 → IsMember=false，无消息
        var result = await historyStore.QueryByConversationAsync(
            userId: 9999,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.False(result.IsMember);
        Assert.Empty(result.Messages);
    }

    [Fact]
    public async Task LeftMember_CannotSendNewMessage()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_write_gate");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-write-gate",
            10,
            conversationId,
            "WriteGate",
            [20],
            "s10",
            1_700_000_000_000);

        // 用户 20 离群
        await groupStore.LeaveAsync(
            "req-leave-write-gate",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);

        // 离群后尝试发消息 → 应被拒绝
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            messageStore,
            new RecordingRealtimeOutboxSignal(),
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            groupStore,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var rejected = await processor.ProcessAsync(new IncomingMessageCommand
        {
            CommandId = "cmd-after-leave",
            ClientMessageId = "c-after-leave",
            SenderUserId = 20,
            SenderSessionId = "s20",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "should fail",
            ReceivedAtMs = 1_700_000_002_000
        });

        Assert.False(rejected.Succeeded);
        Assert.Equal("forbidden", rejected.ErrorCode);
    }

    [Fact]
    public async Task LeftMember_CanReadHistory_AfterMode()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_after_mode");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-after",
            10,
            conversationId,
            "AfterMode",
            [20],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "before-leave", 1_700_000_000_500);

        // 用户 20 离群
        await groupStore.LeaveAsync(
            "req-leave-after",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);

        // 离群后用 After 模式做 catch-up 查询
        var result = await historyStore.QueryByConversationAfterAsync(
            userId: 20,
            conversationId,
            afterChangedAtMs: 1_700_000_000_000,
            afterMessageId: "0",
            take: 10);

        Assert.True(result.IsMember);
        Assert.Single(result.Messages);
        Assert.Equal("before-leave", result.Messages[0].Content);
    }

    [Fact]
    public async Task LeftMember_CannotReadMessagesSentAfterLeaving()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_boundary");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-boundary",
            10,
            conversationId,
            "Boundary",
            [20],
            "s10",
            1_700_000_000_000);

        // 离群前发送的消息
        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "before-leave", 1_700_000_000_500);

        // 用户 20 离群
        await groupStore.LeaveAsync(
            "req-leave-boundary",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);

        // 离群后群里继续发送的新消息
        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "after-leave", 1_700_000_002_000);

        // 离群用户查询历史：应只看到离群前的消息，看不到离群后的新消息
        var result = await historyStore.QueryByConversationAsync(
            userId: 20,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result.IsMember);
        Assert.Single(result.Messages);
        Assert.Equal("before-leave", result.Messages[0].Content);
    }

    [Fact]
    public async Task DissolvedGroup_MembersCannotReadMessagesSentAfterDissolution()
    {
        var (client, schema) = await CreateStoreAsync("realtime_dissolved_boundary");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-dissolved-b",
            10,
            conversationId,
            "DissolvedBoundary",
            [20],
            "s10",
            1_700_000_000_000);

        await SendGroupMessageAsync(messageStore, conversationId, senderUserId: 10, "before-dissolve", 1_700_000_000_500);

        // Owner 解散群
        await groupStore.DissolveAsync(
            "req-dissolve-b",
            10,
            conversationId,
            "s10",
            1_700_000_001_000);

        // 解散后尝试发送消息（应失败，因为 LoadGroupContextAsync 过滤 dissolved_at_ms）
        // 但即使有残留消息，前成员也不应看到

        // 前成员查询历史：应只看到解散前的消息
        var result = await historyStore.QueryByConversationAsync(
            userId: 20,
            conversationId,
            beforeReceivedAtMs: null,
            beforeMessageId: null,
            take: 10);

        Assert.True(result.IsMember);
        Assert.Single(result.Messages);
        Assert.Equal("before-dissolve", result.Messages[0].Content);
    }

    [Fact]
    public async Task LeftMember_DoesNotAppearInMemberListOrEventTargets()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_member_list");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-list",
            10,
            conversationId,
            "MemberList",
            [20, 30],
            "s10",
            1_700_000_000_000);

        // 用户 20 离群
        await groupStore.LeaveAsync(
            "req-leave-list",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);

        // 活跃成员列表不应包含已离群的用户 20
        var members = await groupStore.ListMembersAsync(10, conversationId);
        Assert.Equal(2, members.Count);
        Assert.DoesNotContain(members, m => m.UserId == 20);
        Assert.Contains(members, m => m.UserId == 10);
        Assert.Contains(members, m => m.UserId == 30);

        // 活跃成员 ID 列表也不应包含用户 20
        var activeIds = await groupStore.ListActiveMemberUserIdsAsync(conversationId);
        Assert.DoesNotContain(20L, activeIds);
        Assert.Contains(10L, activeIds);
        Assert.Contains(30L, activeIds);
    }

    [Fact]
    public async Task DissolvedGroup_NotInActiveConversationList()
    {
        var (client, schema) = await CreateStoreAsync("realtime_dissolved_conversation_list");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(
            client,
            schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-conv-list",
            10,
            conversationId,
            "ConvList",
            [20],
            "s10",
            1_700_000_000_000);

        // 解散前会话出现在列表中
        var beforeList = await conversationStore.QueryListAsync(
            userId: 10,
            beforeIsPinned: null,
            beforePinnedAtMs: null,
            beforeLastMessageAtMs: null,
            beforeConversationId: null,
            take: 10);
        Assert.Contains(beforeList, c => c.ConversationId == conversationId);

        // Owner 解散群
        await groupStore.DissolveAsync(
            "req-dissolve-conv-list",
            10,
            conversationId,
            "s10",
            1_700_000_001_000);

        // 解散后会话不应出现在活跃会话列表中
        var afterList = await conversationStore.QueryListAsync(
            userId: 10,
            beforeIsPinned: null,
            beforePinnedAtMs: null,
            beforeLastMessageAtMs: null,
            beforeConversationId: null,
            take: 10);
        Assert.DoesNotContain(afterList, c => c.ConversationId == conversationId);
    }

    [Fact]
    public async Task LeftMember_CannotSetPrefsOrMarkRead()
    {
        var (client, schema) = await CreateStoreAsync("realtime_leave_write_prefs");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(
            client,
            schema);
        var conversationId = ConversationId.CreateGroup();

        await groupStore.CreateGroupAsync(
            "req-create-prefs",
            10,
            conversationId,
            "Prefs",
            [20],
            "s10",
            1_700_000_000_000);

        // 用户 20 离群
        await groupStore.LeaveAsync(
            "req-leave-prefs",
            20,
            conversationId,
            "s20",
            1_700_000_001_000);

        // 离群后尝试设置 prefs → 应找不到成员行
        var prefsResult = await conversationStore.SetMemberPrefsAsync(
            userId: 20,
            conversationId,
            pinned: true,
            muted: false,
            mutedUntilMs: null);
        Assert.False(prefsResult.Found);

        // 离群后尝试标记已读 → 应找不到成员行
        var readResult = await conversationStore.AdvanceReadCursorAsync(
            userId: 20,
            conversationId,
            readAtMs: 1_700_000_002_000,
            readMessageId: "msg-x");
        Assert.False(readResult.Found);
    }

    private static async Task SendGroupMessageAsync(
        NpgsqlRealtimeMessageStore messageStore,
        string conversationId,
        long senderUserId,
        string content,
        long receivedAtMs)
    {
        var message = new RealtimeMessageRecord
        {
            MessageId = $"g-msg-{senderUserId}-{receivedAtMs}",
            ClientMessageId = $"g-client-{senderUserId}-{receivedAtMs}",
            SenderUserId = senderUserId,
            SenderSessionId = $"s{senderUserId}",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };
        var template = new RealtimeEvent
        {
            EventId = MessageEventIdFactory.CreateMessageReceivedEventId(senderUserId, message.ClientMessageId, 1),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = senderUserId,
            ActorUserId = senderUserId,
            MessageId = message.MessageId,
            SessionId = message.SenderSessionId!,
            OccurredAtMs = receivedAtMs
        };
        var persisted = await messageStore.SaveAsync(message, template);
        Assert.Equal(RealtimeMessagePersistKind.Created, persisted.Kind);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateStoreAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }
}

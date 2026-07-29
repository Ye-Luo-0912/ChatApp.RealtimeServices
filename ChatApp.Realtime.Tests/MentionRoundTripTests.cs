using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class MentionRoundTripTests
{
    private const string GroupConversationId = "grp:0123456789abcdef0123456789abcdef";

    [Fact]
    public async Task ProcessAsync_PropagatesSanitizedMentions_ForManagerSender()
    {
        // 发送方 1001 是群 Owner（管理员），可 @all/@admin。
        // 非群成员 9999 与自身 1001 静默移除；重复项去重；结果按升序排列。
        var members = new List<ConversationMemberItem>
        {
            new() { UserId = 1001, Role = ConversationMemberRole.Owner },
            new() { UserId = 2001, Role = ConversationMemberRole.Member },
            new() { UserId = 2002, Role = ConversationMemberRole.Member },
            new() { UserId = 2003, Role = ConversationMemberRole.Admin }
        };
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new FakeGroupStore(members),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var command = ValidGroupCommand() with
        {
            MentionedUserIds = [1001L, 2003, 2001, 9999, 2001, 2002],
            MentionedRoles = ["all", "admin", "all", "unknown"]
        };

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);
        Assert.NotNull(store.Event);

        // 自身(1001)移除、非成员(9999)移除、去重、升序排列
        Assert.Equal(new long[] { 2001, 2002, 2003 }, store.Message!.MentionedUserIds);
        // 白名单外角色(unknown)移除、去重、Ordinal 升序（"admin" < "all"）
        Assert.Equal(new[] { "admin", "all" }, store.Message!.MentionedRoles);

        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event!.Payload);
        Assert.Equal(store.Message.MentionedUserIds, payload.MentionedUserIds);
        Assert.Equal(store.Message.MentionedRoles, payload.MentionedRoles);
    }

    [Fact]
    public async Task ProcessAsync_FiltersManagerOnlyRoles_ForNonManagerSender()
    {
        // 发送方 2001 是普通 Member，不能 @all/@admin；用户 mention 仍生效。
        var members = new List<ConversationMemberItem>
        {
            new() { UserId = 1001, Role = ConversationMemberRole.Owner },
            new() { UserId = 2001, Role = ConversationMemberRole.Member },
            new() { UserId = 2002, Role = ConversationMemberRole.Member }
        };
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new FakeGroupStore(members),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var command = ValidGroupCommand() with
        {
            SenderUserId = 2001,
            MentionedUserIds = [1001L, 2002, 9999],
            MentionedRoles = ["all", "admin"]
        };

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);

        // 非成员 9999 移除、升序
        Assert.Equal(new long[] { 1001, 2002 }, store.Message!.MentionedUserIds);
        // 非管理员的 @all/@admin 全部移除
        Assert.Null(store.Message!.MentionedRoles);

        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event!.Payload);
        Assert.Equal(store.Message.MentionedUserIds, payload.MentionedUserIds);
        Assert.Null(payload.MentionedRoles);
    }

    [Fact]
    public async Task ProcessAsync_RemovesSelfMentionAndDeduplicates()
    {
        var members = new List<ConversationMemberItem>
        {
            new() { UserId = 1001, Role = ConversationMemberRole.Owner },
            new() { UserId = 2001, Role = ConversationMemberRole.Member }
        };
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new FakeGroupStore(members),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var command = ValidGroupCommand() with
        {
            MentionedUserIds = [1001L, 1001, 2001, 2001]
        };

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);
        // 自身全部移除、去重后仅剩 2001
        Assert.Equal(new long[] { 2001 }, store.Message!.MentionedUserIds);
        Assert.Null(store.Message!.MentionedRoles);
    }

    [Fact]
    public async Task ProcessAsync_PropagatesNullMentions_ForGroupWithoutMentions()
    {
        var members = new List<ConversationMemberItem>
        {
            new() { UserId = 1001, Role = ConversationMemberRole.Owner }
        };
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new FakeGroupStore(members),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(ValidGroupCommand());

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);
        Assert.Null(store.Message!.MentionedUserIds);
        Assert.Null(store.Message!.MentionedRoles);

        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event!.Payload);
        Assert.Null(payload.MentionedUserIds);
        Assert.Null(payload.MentionedRoles);
    }

    [Fact]
    public async Task ProcessAsync_PropagatesMentions_ForDirectMessage()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new FakeGroupStore([]),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        // 单聊场景下 mention 仍然透传（Gateway 侧已规整为 null，但 Realtime 侧不二次过滤）
        var command = ValidDirectCommand() with
        {
            MentionedUserIds = [2001L],
            MentionedRoles = ["all"]
        };

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);
        Assert.Equal(command.MentionedUserIds, store.Message!.MentionedUserIds);
        Assert.Equal(command.MentionedRoles, store.Message!.MentionedRoles);

        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event!.Payload);
        Assert.Equal(command.MentionedUserIds, payload.MentionedUserIds);
        Assert.Equal(command.MentionedRoles, payload.MentionedRoles);
    }

    private static IncomingMessageCommand ValidGroupCommand() => new()
    {
        CommandId = "grp-cmd-1",
        ClientMessageId = "grp-client-1",
        SenderUserId = 1001,
        SenderSessionId = "session-1",
        ReceiverUserId = 0,
        ConversationId = GroupConversationId,
        Content = "hello group",
        ReceivedAtMs = 1_700_000_000_000
    };

    private static IncomingMessageCommand ValidDirectCommand() => new()
    {
        CommandId = "dm-cmd-1",
        ClientMessageId = "dm-client-1",
        SenderUserId = 1001,
        SenderSessionId = "session-1",
        ReceiverUserId = 1002,
        Content = "hello dm",
        ReceivedAtMs = 1_700_000_000_000
    };

    private sealed class CapturingStore : IRealtimeMessageStore
    {
        public RealtimeMessageRecord? Message { get; private set; }
        public RealtimeEvent? Event { get; private set; }

        public Task<RealtimeMessagePersistResult> SaveAsync(
            RealtimeMessageRecord message,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default)
        {
            Message = message;
            Event = eventToPublish;
            return Task.FromResult(RealtimeMessagePersistResult.Created(message.MessageId));
        }

        public Task<MessageReceiptPersistResult> ApplyReceiptAsync(
            MessageReceiptRecord receipt,
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.FromResult(
                new MessageReceiptPersistResult(
                    MessageReceiptPersistStatus.Unchanged,
                    receipt.MessageId));

        public Task<MessageRecallPersistResult> ApplyRecallAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            long recalledAtMs,
            long maxAgeMs,
            CancellationToken ct = default) =>
            Task.FromResult(new MessageRecallPersistResult(MessageRecallPersistStatus.NotFound, messageId));

        public Task<MessageEditPersistResult> ApplyEditAsync(
            string requestId,
            string messageId,
            long senderUserId,
            string senderSessionId,
            string content,
            long editedAtMs,
            long maxAgeMs,
            IReadOnlyList<long>? mentionedUserIds = null,
            IReadOnlyList<string>? mentionedRoles = null,
            CancellationToken ct = default) =>
            Task.FromResult(new MessageEditPersistResult(MessageEditPersistStatus.NotFound, messageId));

        public Task<long> DeleteByUserAsync(
            long userId,
            int batchSize = 1000,
            CancellationToken ct = default) =>
            Task.FromResult(0L);

        public Task EnqueueEventAsync(
            RealtimeEvent eventToPublish,
            CancellationToken ct = default) =>
            Task.CompletedTask;
    }

    private sealed class FakeGroupStore : IRealtimeGroupStore
    {
        private readonly IReadOnlyList<ConversationMemberItem> _members;

        public FakeGroupStore(IReadOnlyList<ConversationMemberItem> members)
        {
            _members = members;
        }

        public Task<GroupCreatePersistResult> CreateGroupAsync(
            string requestId,
            long creatorUserId,
            string conversationId,
            string title,
            IReadOnlyList<long> memberUserIds,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> AddMembersAsync(
            string requestId,
            long actorUserId,
            string conversationId,
            IReadOnlyList<long> memberUserIds,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> RemoveMemberAsync(
            string requestId,
            long actorUserId,
            string conversationId,
            long targetUserId,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> LeaveAsync(
            string requestId,
            long actorUserId,
            string conversationId,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> ChangeRoleAsync(
            string requestId,
            long actorUserId,
            string conversationId,
            long targetUserId,
            ConversationMemberRole newRole,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> DissolveAsync(
            string requestId,
            long actorUserId,
            string conversationId,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
            long actorUserId,
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult(_members);

        public Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<long>>(_members.Select(m => m.UserId).ToArray());

        public Task<bool> IsActiveMemberAsync(
            string conversationId,
            long userId,
            CancellationToken ct = default) =>
            Task.FromResult(_members.Any(m => m.UserId == userId));

        public Task<ConversationMemberRole?> GetMemberRoleAsync(
            long userId,
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult(_members.FirstOrDefault(m => m.UserId == userId)?.Role);

        public Task<IReadOnlyList<long>> ValidateMembersAsync(
            string conversationId,
            IReadOnlyList<long> userIds,
            CancellationToken ct = default)
        {
            var memberIds = new HashSet<long>(_members.Select(m => m.UserId));
            var result = userIds.Where(id => memberIds.Contains(id)).OrderBy(id => id).ToArray();
            return Task.FromResult<IReadOnlyList<long>>(result);
        }
    }
}

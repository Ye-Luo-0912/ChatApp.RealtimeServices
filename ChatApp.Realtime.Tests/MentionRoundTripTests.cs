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
    public async Task ProcessAsync_PropagatesMentions_FromCommand_ToMessageRecord_AndEventPayload()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            new AlwaysMemberGroupStore(),
            signal,
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var command = ValidGroupCommand() with
        {
            MentionedUserIds = [2001L, 2002, 2003],
            MentionedRoles = ["all", "admin"]
        };

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.NotNull(store.Message);
        Assert.NotNull(store.Event);

        // 消息记录携带 mention 字段
        Assert.Equal(command.MentionedUserIds, store.Message!.MentionedUserIds);
        Assert.Equal(command.MentionedRoles, store.Message!.MentionedRoles);

        // P1-4：Processor 传 Payload 对象（不预序列化 PayloadJson），Store 负责一次性物化。
        // 这里直接校验 Payload 对象的 mention 字段。
        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event!.Payload);
        Assert.Equal(command.MentionedUserIds, payload.MentionedUserIds);
        Assert.Equal(command.MentionedRoles, payload.MentionedRoles);
    }

    [Fact]
    public async Task ProcessAsync_PropagatesNullMentions_ForGroupWithoutMentions()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            new AlwaysMemberGroupStore(),
            signal,
            metrics,
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
            new AlwaysMemberGroupStore(),
            signal,
            metrics,
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

    private sealed class AlwaysMemberGroupStore : IRealtimeGroupStore
    {
        public Task<GroupCreatePersistResult> CreateGroupAsync(
            long creatorUserId,
            string conversationId,
            string title,
            IReadOnlyList<long> memberUserIds,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> AddMembersAsync(
            long actorUserId,
            string conversationId,
            IReadOnlyList<long> memberUserIds,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> RemoveMemberAsync(
            long actorUserId,
            string conversationId,
            long targetUserId,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> LeaveAsync(
            long actorUserId,
            string conversationId,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<GroupMutatePersistResult> ChangeRoleAsync(
            long actorUserId,
            string conversationId,
            long targetUserId,
            ConversationMemberRole newRole,
            string? actorSessionId,
            long occurredAtMs,
            CancellationToken ct = default) =>
            throw new NotSupportedException();

        public Task<IReadOnlyList<ConversationMemberItem>> ListMembersAsync(
            long actorUserId,
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<ConversationMemberItem>>([]);

        public Task<IReadOnlyList<long>> ListActiveMemberUserIdsAsync(
            string conversationId,
            CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<long>>([]);

        public Task<bool> IsActiveMemberAsync(
            string conversationId,
            long userId,
            CancellationToken ct = default) =>
            Task.FromResult(true);
    }
}

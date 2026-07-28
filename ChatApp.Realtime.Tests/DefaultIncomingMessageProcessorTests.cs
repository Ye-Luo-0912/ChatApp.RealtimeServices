using System.Diagnostics;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests;

public sealed class DefaultIncomingMessageProcessorTests
{
    [Fact]
    public async Task ProcessAsync_PersistsMessageAndOutboxEventTogether()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);
        var command = ValidCommand();

        var result = await processor.ProcessAsync(command);

        Assert.True(result.Succeeded);
        Assert.Equal(command.CommandId, result.MessageId);
        Assert.Equal(1, signal.Notifications);
        Assert.NotNull(store.Message);
        Assert.NotNull(store.Event);
        Assert.Equal(command.CommandId, store.Event.MessageId);
        Assert.Equal(RealtimeEventType.MessageReceived, store.Event.Type);
        Assert.Equal(command.ReceiverUserId, store.Event.TargetUserId);
        // P1-4：Processor 不再预先序列化 PayloadJson，而是把 payload 对象通过 Payload 传给 Store。
        // Store（Npgsql/EfCore）会在附件绑定后调用 EnrichChatMessagePayload 一次性物化。
        Assert.Null(store.Event.PayloadJson);
        var payload = Assert.IsType<RealtimeChatMessagePayload>(store.Event.Payload);
        Assert.Equal(command.Content, payload.Content);
        Assert.Equal(command.ClientMessageId, payload.ClientMessageId);
        Assert.Equal("dm:1001:1002", payload.ConversationId);
        Assert.Equal("dm:1001:1002", store.Message!.ConversationId);
    }

    [Fact]
    public async Task ProcessAsync_PersistsCurrentTraceContextInOutboxEvent()
    {
        using var activity = new Activity("test")
            .SetIdFormat(ActivityIdFormat.W3C)
            .Start();
        var store = new CapturingStore();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            new RecordingRealtimeOutboxSignal(),
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        await processor.ProcessAsync(ValidCommand());

        Assert.Equal(activity.Id, store.Event!.TraceParent);
        Assert.Equal(activity.TraceStateString, store.Event.TraceState);
    }

    [Fact]
    public async Task ProcessAsync_CreatesDeterministicEventIdForRetries()
    {
        var firstStore = new CapturingStore();
        var secondStore = new CapturingStore();
        using var firstMetrics = new RealtimeMetrics();
        using var secondMetrics = new RealtimeMetrics();
        var command = ValidCommand();

        await new DefaultIncomingMessageProcessor(
            firstStore,
            new RecordingRealtimeOutboxSignal(),
            firstMetrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance).ProcessAsync(command);
        await new DefaultIncomingMessageProcessor(
            secondStore,
            new RecordingRealtimeOutboxSignal(),
            secondMetrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance).ProcessAsync(command);

        Assert.Equal(firstStore.Event!.EventId, secondStore.Event!.EventId);
        Assert.Equal(64, firstStore.Event.EventId.Length);
    }

    [Fact]
    public async Task ProcessAsync_NotifiesOutboxForIdempotentMessage()
    {
        var store = new CapturingStore(isNew: false);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.True(result.Succeeded);
        Assert.Equal(1, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_RejectsPermanentValidationErrorBeforeStorage()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);
        var command = ValidCommand() with { Content = " " };

        var result = await processor.ProcessAsync(command);

        Assert.False(result.Succeeded);
        Assert.Equal(MessageFailureKind.Permanent, result.FailureKind);
        Assert.Equal("empty_content", result.ErrorCode);
        Assert.Equal(0, signal.Notifications);
        Assert.Null(store.Message);
    }

    [Fact]
    public async Task ProcessAsync_RejectsIdempotencyContentConflict()
    {
        var store = new CapturingStore(conflict: true);
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var result = await processor.ProcessAsync(ValidCommand());

        Assert.False(result.Succeeded);
        Assert.Equal("idempotency_conflict", result.ErrorCode);
        Assert.Equal(MessageFailureKind.Permanent, result.FailureKind);
        Assert.Equal(0, signal.Notifications);
    }

    [Fact]
    public async Task ProcessAsync_RejectsSelfChat()
    {
        var store = new CapturingStore();
        var signal = new RecordingRealtimeOutboxSignal();
        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            store,
            signal,
            metrics,
            NoopTombstoneAndLedger.Tombstone,
            new AlwaysMemberGroupStore(),
            NullLogger<DefaultIncomingMessageProcessor>.Instance);
        var command = ValidCommand() with { ReceiverUserId = 1001 };

        var result = await processor.ProcessAsync(command);

        Assert.False(result.Succeeded);
        Assert.Equal("invalid_self_chat", result.ErrorCode);
        Assert.Equal(0, signal.Notifications);
        Assert.Null(store.Message);
    }

    private static IncomingMessageCommand ValidCommand() => new()
    {
        CommandId = "command-1",
        ClientMessageId = "client-1",
        SenderUserId = 1001,
        SenderSessionId = "session-1",
        ReceiverUserId = 1002,
        Content = "hello",
        ReceivedAtMs = 1_700_000_000_000
    };

    private sealed class CapturingStore(bool isNew = true, bool conflict = false) : IRealtimeMessageStore
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
            if (conflict)
                return Task.FromResult(RealtimeMessagePersistResult.Conflict(message.MessageId));
            return Task.FromResult(
                isNew
                    ? RealtimeMessagePersistResult.Created(message.MessageId)
                    : RealtimeMessagePersistResult.Duplicate(message.MessageId));
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

    private sealed class AlwaysMemberGroupStore : IRealtimeGroupStore
    {
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

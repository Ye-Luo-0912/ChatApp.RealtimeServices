using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class MessageReactionStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task AddRemove_IsIdempotent_AndRejectsNonMemberAndRecalled()
    {
        var (messageStore, reactionStore, historyStore, conversationId, messageId) =
            await SeedAsync("realtime_reactions");

        var options = new MessageReactionOptions();

        var first = await reactionStore.AddAsync(
            messageId,
            actorUserId: 1001,
            actorSessionId: "s1",
            emoji: "👍",
            occurredAtMs: 2_000,
            options);
        Assert.Equal(MessageReactionPersistStatus.Applied, first.Status);
        Assert.Equal(1, first.EmojiCount);

        var second = await reactionStore.AddAsync(
            messageId,
            actorUserId: 1001,
            actorSessionId: "s1",
            emoji: "👍",
            occurredAtMs: 2_001,
            options);
        Assert.Equal(MessageReactionPersistStatus.Unchanged, second.Status);

        var missingRemove = await reactionStore.RemoveAsync(
            messageId,
            actorUserId: 1001,
            actorSessionId: "s1",
            emoji: "🔥",
            occurredAtMs: 2_002);
        Assert.Equal(MessageReactionPersistStatus.Unchanged, missingRemove.Status);

        var outsider = await reactionStore.AddAsync(
            messageId,
            actorUserId: 9999,
            actorSessionId: "s9",
            emoji: "👍",
            occurredAtMs: 2_003,
            options);
        Assert.Equal(MessageReactionPersistStatus.NotAllowed, outsider.Status);

        var removed = await reactionStore.RemoveAsync(
            messageId,
            actorUserId: 1001,
            actorSessionId: "s1",
            emoji: "👍",
            occurredAtMs: 2_004);
        Assert.Equal(MessageReactionPersistStatus.Applied, removed.Status);
        Assert.Equal(0, removed.EmojiCount);

        await messageStore.ApplyRecallAsync(
            requestId: "recall-1",
            messageId,
            senderUserId: 1001,
            senderSessionId: "s1",
            recalledAtMs: 3_000,
            maxAgeMs: 60_000);

        var afterRecall = await reactionStore.AddAsync(
            messageId,
            actorUserId: 1002,
            actorSessionId: "s2",
            emoji: "👍",
            occurredAtMs: 3_001,
            options);
        Assert.Equal(MessageReactionPersistStatus.AlreadyRecalled, afterRecall.Status);

        _ = (historyStore, conversationId);
    }

    [Fact]
    public async Task Reaction_BumpsChangedAt_AndHistoryEnrichmentIncludesSummary()
    {
        var (messageStore, reactionStore, historyStore, conversationId, messageId) =
            await SeedAsync("realtime_reactions_sync");

        var before = await historyStore.TryGetByIdAsync(messageId);
        Assert.NotNull(before);
        Assert.Equal(1_000, before!.ChangedAtMs);

        var applied = await reactionStore.AddAsync(
            messageId,
            actorUserId: 1002,
            actorSessionId: "s2",
            emoji: "🎉",
            occurredAtMs: 5_000,
            new MessageReactionOptions());
        Assert.Equal(MessageReactionPersistStatus.Applied, applied.Status);

        var after = await historyStore.TryGetByIdAsync(messageId);
        Assert.NotNull(after);
        Assert.Equal(5_000, after!.ChangedAtMs);

        var catchUp = await historyStore.QueryByConversationAfterAsync(
            userId: 1001,
            conversationId,
            afterChangedAtMs: 1_000,
            afterMessageId: messageId,
            take: 10);
        Assert.True(catchUp.IsMember);
        Assert.Contains(catchUp.Messages, m => m.MessageId == messageId);

        var processor = new DefaultMessageHistoryQueryProcessor(
            historyStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance),
            reactionStore);
        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "hist-1",
            UserId = 1001,
            MessageId = messageId
        });
        Assert.True(page.Succeeded);
        var summary = Assert.Single(page.Items[0].Reactions!);
        Assert.Equal("🎉", summary.Emoji);
        Assert.Equal(1, summary.Count);
        Assert.False(summary.ReactedByMe);

        _ = messageStore;
    }

    private async Task<(
        NpgsqlRealtimeMessageStore MessageStore,
        NpgsqlRealtimeReactionStore ReactionStore,
        NpgsqlRealtimeMessageHistoryStore HistoryStore,
        string ConversationId,
        string MessageId)> SeedAsync(string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);

        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var reactionStore = new NpgsqlRealtimeReactionStore(client, schema, TestMutationPolicy.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateDirect(1001, 1002);
        const string messageId = "msg-react-1";

        await messageStore.SaveAsync(
            new RealtimeMessageRecord
            {
                MessageId = messageId,
                ClientMessageId = "client-react-1",
                SenderUserId = 1001,
                SenderSessionId = "s1",
                ReceiverUserId = 1002,
                ConversationId = conversationId,
                Content = "hello",
                ReceivedAtMs = 1_000
            },
            new RealtimeEvent
            {
                EventId = "evt-react-1",
                Type = RealtimeEventType.MessageReceived,
                TargetUserId = 1002,
                MessageId = messageId,
                OccurredAtMs = 1_000
            });

        return (messageStore, reactionStore, historyStore, conversationId, messageId);
    }
}

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class ConversationHistoryPaginationTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task ConversationHistory_KeysetPages_HaveNoOverlapOrGaps()
    {
        var (messageStore, historyStore, conversationId) = await SeedAsync("realtime_hist_pages");
        var processor = new DefaultMessageHistoryQueryProcessor(
            historyStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var seen = new HashSet<string>(StringComparer.Ordinal);
        MessageHistoryCursor? cursor = null;
        var pages = 0;

        while (true)
        {
            var page = await processor.ProcessAsync(new MessageHistoryQuery
            {
                RequestId = $"page-{pages}",
                UserId = 1001,
                ConversationId = conversationId,
                BeforeReceivedAtMs = cursor?.ReceivedAtMs,
                BeforeMessageId = cursor?.MessageId,
                Limit = 3
            });

            Assert.True(page.Succeeded, page.ErrorMessage);
            pages++;
            foreach (var item in page.Items)
            {
                Assert.Equal(conversationId, item.ConversationId);
                Assert.True(seen.Add(item.MessageId), $"重复消息 {item.MessageId}");
            }

            if (!page.HasMore)
                break;

            Assert.NotNull(page.NextCursor);
            cursor = page.NextCursor;
            Assert.True(pages < 20);
        }

        Assert.Equal(10, seen.Count);
        Assert.Equal(4, pages);

        // 用户级历史仍可用，且不应包含其他会话消息过滤逻辑错误。
        var global = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "global",
            UserId = 1001,
            Limit = 50
        });
        Assert.True(global.Succeeded);
        Assert.True(global.Items.Count >= 10);

        _ = messageStore;
    }

    [Fact]
    public async Task ConversationHistory_SameMillisecond_OrdersByMessageId()
    {
        var (_, historyStore, conversationId) = await SeedSameMillisecondAsync("realtime_hist_tie");
        var processor = new DefaultMessageHistoryQueryProcessor(
            historyStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "tie",
            UserId = 7,
            ConversationId = conversationId,
            Limit = 10
        });

        Assert.True(page.Succeeded);
        Assert.Equal(3, page.Items.Count);
        Assert.Equal(["msg-c", "msg-b", "msg-a"], page.Items.Select(i => i.MessageId).ToArray());
    }

    [Fact]
    public async Task ConversationHistory_NonMember_IsForbidden()
    {
        var (_, historyStore, conversationId) = await SeedAsync("realtime_hist_forbid");
        var processor = new DefaultMessageHistoryQueryProcessor(
            historyStore,
            new NoopRealtimeAttachmentStore(NullLogger<NoopRealtimeAttachmentStore>.Instance));

        var page = await processor.ProcessAsync(new MessageHistoryQuery
        {
            RequestId = "forbid",
            UserId = 9999,
            ConversationId = conversationId,
            Limit = 10
        });

        Assert.False(page.Succeeded);
        Assert.Equal("forbidden", page.ErrorCode);
    }

    private async Task<(NpgsqlRealtimeMessageStore MessageStore, NpgsqlRealtimeMessageHistoryStore HistoryStore, string ConversationId)>
        SeedAsync(string schemaName)
    {
        var (client, schema) = await CreateSchemaAsync(schemaName);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateDirect(1001, 1002);

        for (var i = 0; i < 10; i++)
        {
            var messageId = $"msg-{i:D2}";
            await messageStore.SaveAsync(
                new RealtimeMessageRecord
                {
                    MessageId = messageId,
                    ClientMessageId = $"client-{i}",
                    SenderUserId = 1001,
                    SenderSessionId = "s1",
                    ReceiverUserId = 1002,
                    ConversationId = conversationId,
                    Content = $"body-{i}",
                    ReceivedAtMs = 1_000 + i
                },
                new RealtimeEvent
                {
                    EventId = $"evt-{i}",
                    Type = RealtimeEventType.MessageReceived,
                    TargetUserId = 1002,
                    MessageId = messageId,
                    OccurredAtMs = 1_000 + i
                });
        }

        return (messageStore, historyStore, conversationId);
    }

    private async Task<(NpgsqlRealtimeMessageStore MessageStore, NpgsqlRealtimeMessageHistoryStore HistoryStore, string ConversationId)>
        SeedSameMillisecondAsync(string schemaName)
    {
        var (client, schema) = await CreateSchemaAsync(schemaName);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var historyStore = new NpgsqlRealtimeMessageHistoryStore(client, schema);
        var conversationId = ConversationId.CreateDirect(7, 8);

        foreach (var (id, content) in new[]
                 {
                     ("msg-a", "a"),
                     ("msg-b", "b"),
                     ("msg-c", "c")
                 })
        {
            await messageStore.SaveAsync(
                new RealtimeMessageRecord
                {
                    MessageId = id,
                    ClientMessageId = $"client-{id}",
                    SenderUserId = 7,
                    SenderSessionId = "s1",
                    ReceiverUserId = 8,
                    ConversationId = conversationId,
                    Content = content,
                    ReceivedAtMs = 50
                },
                new RealtimeEvent
                {
                    EventId = $"evt-{id}",
                    Type = RealtimeEventType.MessageReceived,
                    TargetUserId = 8,
                    MessageId = id,
                    OccurredAtMs = 50
                });
        }

        return (messageStore, historyStore, conversationId);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateSchemaAsync(
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

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Integration.Serialization;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class ConversationProjectionTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task SaveAsync_CreatesConversationProjectionAndEvents()
    {
        var (client, schema) = await CreateStoreAsync("realtime_conv_create");
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        var conversationId = ConversationId.CreateDirect(1001, 1002);
        var result = await store.SaveAsync(
            CreateMessage("msg-1", 1001, 1002, conversationId, "hello", 100),
            CreateMessageReceivedEvent("evt-1", 1002, "msg-1", 100));

        Assert.Equal(RealtimeMessagePersistKind.Created, result.Kind);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using (var command = new NpgsqlCommand(
                           $"""
                            SELECT last_message_id, last_message_preview, last_message_at_ms, last_sender_user_id
                            FROM {schema.ConversationsTableSql}
                            WHERE conversation_id = @conversation_id
                            """,
                           connection))
        {
            command.Parameters.AddWithValue("conversation_id", conversationId);
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal("msg-1", reader.GetString(0));
            Assert.Equal("hello", reader.GetString(1));
            Assert.Equal(100, reader.GetInt64(2));
            Assert.Equal(1001, reader.GetInt64(3));
        }

        await using (var members = new NpgsqlCommand(
                           $"SELECT COUNT(*) FROM {schema.ConversationMembersTableSql} WHERE conversation_id = @conversation_id",
                           connection))
        {
            members.Parameters.AddWithValue("conversation_id", conversationId);
            Assert.Equal(2, Convert.ToInt32(await members.ExecuteScalarAsync()));
        }

        await using (var outbox = new NpgsqlCommand(
                           $"""
                            SELECT event_type, target_user_id, payload_json
                            FROM {schema.OutboxTableSql}
                            WHERE event_type = @event_type
                            ORDER BY target_user_id
                            """,
                           connection))
        {
            outbox.Parameters.AddWithValue("event_type", (short)RealtimeEventType.ConversationListChanged);
            await using var reader = await outbox.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1001, reader.GetInt64(1));
            var evtA = RealtimeWireSerializer.DeserializeEvent(reader.GetString(2));
            Assert.NotNull(evtA);
            var payloadA = RealtimeWireSerializer.DeserializeConversationChanged(evtA.PayloadJson!);
            Assert.NotNull(payloadA);
            Assert.Equal(conversationId, payloadA.ConversationId);
            Assert.Equal(1002, payloadA.PeerUserId);

            Assert.True(await reader.ReadAsync());
            Assert.Equal(1002, reader.GetInt64(1));
            Assert.False(await reader.ReadAsync());
        }
    }

    [Fact]
    public async Task SaveAsync_OutOfOrderMessage_DoesNotRegressLastMessage()
    {
        var (client, schema) = await CreateStoreAsync("realtime_conv_ooo");
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationId = ConversationId.CreateDirect(1, 2);

        await store.SaveAsync(
            CreateMessage("msg-new", 1, 2, conversationId, "newer", 200),
            CreateMessageReceivedEvent("evt-new", 2, "msg-new", 200));
        await store.SaveAsync(
            CreateMessage("msg-old", 1, 2, conversationId, "older", 100),
            CreateMessageReceivedEvent("evt-old", 2, "msg-old", 100));

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT last_message_id, last_message_preview, last_message_at_ms
             FROM {schema.ConversationsTableSql}
             WHERE conversation_id = @conversation_id
             """,
            connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal("msg-new", reader.GetString(0));
        Assert.Equal("newer", reader.GetString(1));
        Assert.Equal(200, reader.GetInt64(2));
    }

    [Fact]
    public async Task SaveAsync_SameMillisecond_UsesMessageIdTieBreak()
    {
        var (client, schema) = await CreateStoreAsync("realtime_conv_tie");
        var store = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            TestMutationPolicy.Instance,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationId = ConversationId.CreateDirect(3, 4);

        await store.SaveAsync(
            CreateMessage("msg-a", 3, 4, conversationId, "a", 50),
            CreateMessageReceivedEvent("evt-a", 4, "msg-a", 50));
        await store.SaveAsync(
            CreateMessage("msg-b", 3, 4, conversationId, "b", 50),
            CreateMessageReceivedEvent("evt-b", 4, "msg-b", 50));

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT last_message_id FROM {schema.ConversationsTableSql} WHERE conversation_id = @conversation_id",
            connection);
        command.Parameters.AddWithValue("conversation_id", conversationId);
        Assert.Equal("msg-b", await command.ExecuteScalarAsync());
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

    private static RealtimeMessageRecord CreateMessage(
        string messageId,
        long sender,
        long receiver,
        string conversationId,
        string content,
        long receivedAtMs) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "session-1",
            ReceiverUserId = receiver,
            ConversationId = conversationId,
            Content = content,
            ReceivedAtMs = receivedAtMs
        };

    private static RealtimeEvent CreateMessageReceivedEvent(
        string eventId,
        long targetUserId,
        string messageId,
        long occurredAtMs) =>
        new()
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = targetUserId,
            MessageId = messageId,
            OccurredAtMs = occurredAtMs
        };
}

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using System.Text.Json;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class GroupChatTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public void CreateGroup_IdIsValid()
    {
        var id = ConversationId.CreateGroup();
        Assert.True(ConversationId.IsGroup(id));
        Assert.False(ConversationId.IsDirect(id));
        Assert.StartsWith("grp:", id);
        Assert.Equal(36, id.Length);
    }

    [Fact]
    public async Task CreateGroup_AddsOwnerAndMembers_EmitsEvents()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_create");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        var result = await groupStore.CreateGroupAsync(
            creatorUserId: 1001,
            conversationId,
            title: "Squad",
            memberUserIds: [1002, 1003],
            actorSessionId: "s1",
            occurredAtMs: 1_700_000_000_000);

        Assert.True(result.Succeeded);
        Assert.Equal(3, result.Members!.Count);
        Assert.Contains(result.Members, m => m.UserId == 1001 && m.Role == ConversationMemberRole.Owner);

        var members = await groupStore.ListMembersAsync(1001, conversationId);
        Assert.Equal(3, members.Count);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        // P0-1 聚合投递：建群不再产生逐成员 MemberJoined，而是 ConversationListChanged + MembersAdded。
        await using var outbox = new NpgsqlCommand(
            $"""
             SELECT event_type, target_user_ids
             FROM {schema.OutboxTableSql}
             WHERE event_type IN (@changed, @members_added, @joined)
             """,
            connection);
        outbox.Parameters.AddWithValue("changed", (short)RealtimeEventType.ConversationListChanged);
        outbox.Parameters.AddWithValue("members_added", (short)RealtimeEventType.MembersAdded);
        outbox.Parameters.AddWithValue("joined", (short)RealtimeEventType.MemberJoined);

        var changedCount = 0;
        var membersAddedCount = 0;
        var joinedCount = 0;
        long[]? membersAddedTargets = null;
        await using (var reader = await outbox.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                var type = (RealtimeEventType)reader.GetInt16(0);
                if (type == RealtimeEventType.ConversationListChanged)
                    changedCount++;
                else if (type == RealtimeEventType.MembersAdded)
                {
                    membersAddedCount++;
                    if (!reader.IsDBNull(1))
                        membersAddedTargets = (long[])reader.GetValue(1);
                }
                else if (type == RealtimeEventType.MemberJoined)
                    joinedCount++;
            }
        }

        Assert.Equal(1, changedCount);
        Assert.Equal(1, membersAddedCount);
        Assert.Equal(0, joinedCount); // 不再产生 O(N²) 的逐成员事件
        Assert.NotNull(membersAddedTargets);
        Array.Sort(membersAddedTargets!);
        Assert.Equal([1001L, 1002L, 1003L], membersAddedTargets);
    }

    [Fact]
    public async Task CreateGroup_LargeGroup_AggregatesFanOut()
    {
        // P0-1 验证：满 50 人建群只产生 2 个 Outbox 事件（ConversationCreated + MembersAdded），
        // 不再是 1 + 50×50 = 2501 个事件。
        var (client, schema) = await CreateStoreAsync("realtime_group_aggregation");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationId = ConversationId.CreateGroup();

        var memberIds = Enumerable.Range(2000, 49).Select(i => (long)i).ToArray();
        var result = await groupStore.CreateGroupAsync(
            creatorUserId: 1001,
            conversationId,
            title: "Big",
            memberUserIds: memberIds,
            actorSessionId: "s1",
            occurredAtMs: 1_700_000_000_000);

        Assert.True(result.Succeeded);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT COUNT(*)::int, COUNT(*) FILTER (WHERE event_type = @joined)::int
             FROM {schema.OutboxTableSql}
             """,
            connection);
        cmd.Parameters.AddWithValue("joined", (short)RealtimeEventType.MemberJoined);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        var totalEvents = reader.GetInt32(0);
        var joinedEvents = reader.GetInt32(1);

        Assert.Equal(2, totalEvents); // ConversationCreated + MembersAdded
        Assert.Equal(0, joinedEvents); // 不再产生 O(N²) 的 MemberJoined
    }

    [Fact]
    public async Task NonMember_CannotSend_GroupMessage()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_send_gate");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            1001,
            conversationId,
            "G",
            [1002],
            "s1",
            1_700_000_000_000);

        using var metrics = new RealtimeMetrics();
        var processor = new DefaultIncomingMessageProcessor(
            messageStore,
            groupStore,
            new RecordingRealtimeOutboxSignal(),
            metrics,
            NullLogger<DefaultIncomingMessageProcessor>.Instance);

        var rejected = await processor.ProcessAsync(new IncomingMessageCommand
        {
            CommandId = "cmd-out",
            ClientMessageId = "c-out",
            SenderUserId = 9999,
            SenderSessionId = "s-out",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "nope",
            ReceivedAtMs = 1_700_000_000_100
        });
        Assert.False(rejected.Succeeded);
        Assert.Equal("forbidden", rejected.ErrorCode);

        var accepted = await processor.ProcessAsync(new IncomingMessageCommand
        {
            CommandId = "cmd-in",
            ClientMessageId = "c-in",
            SenderUserId = 1001,
            SenderSessionId = "s-in",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "hello group",
            ReceivedAtMs = 1_700_000_000_200
        });
        Assert.True(accepted.Succeeded);
    }

    [Fact]
    public async Task GroupMessage_FanOut_TargetsAllMembers()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_fanout");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);
        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            1,
            conversationId,
            "Fan",
            [2, 3],
            "s1",
            1_700_000_000_000);

        var message = new RealtimeMessageRecord
        {
            MessageId = "g-msg-1",
            ClientMessageId = "g-client-1",
            SenderUserId = 1,
            SenderSessionId = "s1",
            ReceiverUserId = 0,
            ConversationId = conversationId,
            Content = "hi all",
            ReceivedAtMs = 1_700_000_000_500
        };
        var template = new RealtimeEvent
        {
            EventId = RealtimeEventContracts.CreateMessageReceivedEventId(1, "g-client-1", 1),
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = 1,
            ActorUserId = 1,
            MessageId = "g-msg-1",
            SessionId = "s1",
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = "g-msg-1",
                    ClientMessageId = "g-client-1",
                    SenderUserId = 1,
                    SenderSessionId = "s1",
                    ReceiverUserId = 0,
                    ConversationId = conversationId,
                    Content = "hi all",
                    ReceivedAtMs = 1_700_000_000_500
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = 1_700_000_000_500
        };

        var persisted = await messageStore.SaveAsync(message, template);
        Assert.Equal(RealtimeMessagePersistKind.Created, persisted.Kind);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var outbox = new NpgsqlCommand(
            $"""
             SELECT target_user_ids
             FROM {schema.OutboxTableSql}
             WHERE event_type = @type
             """,
            connection);
        outbox.Parameters.AddWithValue("type", (short)RealtimeEventType.MessageReceived);
        await using var reader = await outbox.ExecuteReaderAsync();
        long[]? aggregated = null;
        while (await reader.ReadAsync())
        {
            if (!reader.IsDBNull(0))
            {
                aggregated = (long[])reader.GetValue(0);
                break;
            }
        }

        Assert.NotNull(aggregated);
        Array.Sort(aggregated!);
        Assert.Equal([1L, 2L, 3L], aggregated);
    }

    [Fact]
    public async Task RemoveMember_LosesAccess_RoleChecksWork()
    {
        var (client, schema) = await CreateStoreAsync("realtime_group_roles");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var processor = new DefaultGroupConversationProcessor(
            groupStore,
            new RecordingRealtimeOutboxSignal());
        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            10,
            conversationId,
            "Roles",
            [20, 30],
            "s1",
            1_700_000_000_000);

        var promote = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r1",
            ActorUserId = 10,
            Operation = GroupConversationOperation.ChangeRole,
            ConversationId = conversationId,
            TargetUserId = 20,
            NewRole = ConversationMemberRole.Admin
        });
        Assert.True(promote.Succeeded);

        var memberRemoveAdmin = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r2",
            ActorUserId = 30,
            Operation = GroupConversationOperation.RemoveMember,
            ConversationId = conversationId,
            TargetUserId = 20
        });
        Assert.False(memberRemoveAdmin.Succeeded);
        Assert.Equal("forbidden", memberRemoveAdmin.ErrorCode);

        var remove = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r3",
            ActorUserId = 10,
            Operation = GroupConversationOperation.RemoveMember,
            ConversationId = conversationId,
            TargetUserId = 30
        });
        Assert.True(remove.Succeeded);
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 30));

        var leaveOwner = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r4",
            ActorUserId = 10,
            Operation = GroupConversationOperation.Leave,
            ConversationId = conversationId
        });
        Assert.False(leaveOwner.Succeeded);
        Assert.Equal("owner_must_transfer", leaveOwner.ErrorCode);

        var transfer = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r5",
            ActorUserId = 10,
            Operation = GroupConversationOperation.ChangeRole,
            ConversationId = conversationId,
            TargetUserId = 20,
            NewRole = ConversationMemberRole.Owner
        });
        Assert.True(transfer.Succeeded);

        var leave = await processor.ProcessAsync(new GroupConversationCommand
        {
            RequestId = "r6",
            ActorUserId = 10,
            Operation = GroupConversationOperation.Leave,
            ConversationId = conversationId
        });
        Assert.True(leave.Succeeded);
        Assert.False(await groupStore.IsActiveMemberAsync(conversationId, 10));
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

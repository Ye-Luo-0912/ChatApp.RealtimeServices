using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using System.Text.Json;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// P0-2：用户生命周期 advisory lock 竞态集成测试。
/// <para>
/// 验证 PostgreSQL advisory lock 在用户删除与消息写入之间的互斥语义：
/// - 排他锁（账号删除路径）阻塞同一用户的共享锁（消息写入路径）；
/// - 多个共享锁可并发持有（多个写入互不阻塞）；
/// - tombstone 写入后，新写入被拒绝（AcquireSharedAndCheckActiveAsync 返回 false）；
/// - RecordDeletionAsync 幂等（重复调用不报错）。
/// </para>
/// <para>
/// 使用与 UserLifecycleAdvisoryLock 相同的 namespace 键值（0x5553_4552_4C49_4645 XOR user_id）
/// 通过原生 SQL 复现 advisory lock 语义，避免依赖 internal 类型。
/// </para>
/// </summary>
[Trait("Category", "Integration")]
public sealed class LifecycleRaceTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer? _postgres = string.IsNullOrEmpty(ExternalPostgresConnectionString()) ? new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build() : null;
    private readonly string _externalPostgres = ExternalPostgresConnectionString() ?? string.Empty;
    private readonly string _schemaSuffix = Guid.NewGuid().ToString("N")[..8];

    private static string? ExternalPostgresConnectionString() => Environment.GetEnvironmentVariable("CHATAPP_TEST_POSTGRES");

    private string PostgresConnectionString => _postgres?.GetConnectionString() ?? _externalPostgres;

    public Task InitializeAsync() => _postgres?.StartAsync() ?? Task.CompletedTask;

    public Task DisposeAsync() => _postgres?.DisposeAsync().AsTask() ?? Task.CompletedTask;

    /// <summary>
    /// 与 UserLifecycleAdvisoryLock.NamespaceKey 保持一致，确保测试使用同一锁命名空间。
    /// </summary>
    private const long LifecycleNamespaceKey = 0x5553_4552_4C49_4645L;

    [Fact]
    public async Task ExclusiveLock_BlocksSharedLock_OnSameUser()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_exclusive_blocks_shared");

        const long userId = 9_400_000_001L;
        var lockKey = LifecycleNamespaceKey ^ userId;

        // 连接 A：获取排他锁并保持
        await using var connectionA = await client.GetDataSource().OpenConnectionAsync();
        await using var transactionA = await connectionA.BeginTransactionAsync();
        await AcquireExclusiveLockAsync(connectionA, transactionA, lockKey);

        // 连接 B：尝试获取共享锁，应被阻塞
        await using var connectionB = await client.GetDataSource().OpenConnectionAsync();
        await using var transactionB = await connectionB.BeginTransactionAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
        var sharedLockTask = AcquireSharedLockAsync(connectionB, transactionB, lockKey, cts.Token);

        // 任务应在超时前被阻塞（未完成）
        await Task.Delay(TimeSpan.FromMilliseconds(200));
        Assert.False(sharedLockTask.IsCompleted);

        // 释放排他锁（回滚事务 A）
        await transactionA.RollbackAsync();

        // 共享锁现在应能获取
        await sharedLockTask.WaitAsync(TimeSpan.FromSeconds(2));
        await transactionB.RollbackAsync();
    }

    [Fact]
    public async Task SharedLocks_AreConcurrent_AmongMultipleWriters()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_shared_concurrent");

        const long userId = 9_400_000_011L;
        var lockKey = LifecycleNamespaceKey ^ userId;

        // 5 个并发事务同时获取同一用户的共享锁，全部应在短时间内完成
        var tasks = Enumerable.Range(0, 5).Select(async _ =>
        {
            await using var connection = await client.GetDataSource().OpenConnectionAsync();
            await using var transaction = await connection.BeginTransactionAsync();
            await AcquireSharedLockAsync(connection, transaction, lockKey);
            // 模拟写入耗时
            await Task.Delay(TimeSpan.FromMilliseconds(50));
            await transaction.CommitAsync();
        }).ToArray();

        var allDone = Task.WhenAll(tasks);
        var winner = await Task.WhenAny(
            allDone,
            Task.Delay(TimeSpan.FromSeconds(3)));

        Assert.Same(allDone, winner);
        await allDone;
    }

    [Fact]
    public async Task Tombstone_DeletedState_RejectsNewWrites()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_deleted_rejects_writes");
        var tombstoneStore = new NpgsqlUserDeletionTombstoneStore(
            client,
            schema,
            NullLogger<NpgsqlUserDeletionTombstoneStore>.Instance);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            new PostgresConversationMessageMutationPolicy(
                NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        const long senderUserId = 9_400_000_021L;
        const long receiverUserId = 9_400_000_022L;

        // 写入 Deleting tombstone
        await tombstoneStore.RecordDeletionAsync(
            senderUserId,
            deletionEventId: "evt-deleting-1",
            deletedAtMs: 1_700_000_000_000);

        // Deleting 状态：SaveAsync 应被拒绝
        var message = CreateDirectMessage("msg-rejected-1", senderUserId, receiverUserId);
        var evt = CreateDirectMessageEvent("evt-rejected-1", senderUserId, receiverUserId, message.MessageId);
        var result = await messageStore.SaveAsync(message, evt);
        Assert.Equal(RealtimeMessagePersistKind.UserDeleted, result.Kind);

        // 升级到 Deleted 状态
        await tombstoneStore.RecordDeletionCompletedAsync(senderUserId);

        // Deleted 状态：SaveAsync 仍应被拒绝
        var message2 = CreateDirectMessage("msg-rejected-2", senderUserId, receiverUserId);
        var evt2 = CreateDirectMessageEvent("evt-rejected-2", senderUserId, receiverUserId, message2.MessageId);
        var result2 = await messageStore.SaveAsync(message2, evt2);
        Assert.Equal(RealtimeMessagePersistKind.UserDeleted, result2.Kind);

        // 接收方为已注销用户也应被拒绝
        var message3 = CreateDirectMessage("msg-rejected-3", receiverUserId, senderUserId);
        var evt3 = CreateDirectMessageEvent("evt-rejected-3", receiverUserId, senderUserId, message3.MessageId);
        var result3 = await messageStore.SaveAsync(message3, evt3);
        Assert.Equal(RealtimeMessagePersistKind.UserDeleted, result3.Kind);
    }

    [Fact]
    public async Task RecordDeletion_IsIdempotent_OnRepeatedCalls()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_deletion_idempotent");
        var tombstoneStore = new NpgsqlUserDeletionTombstoneStore(
            client,
            schema,
            NullLogger<NpgsqlUserDeletionTombstoneStore>.Instance);

        const long userId = 9_400_000_031L;

        await tombstoneStore.RecordDeletionAsync(
            userId,
            deletionEventId: "evt-idempotent-1",
            deletedAtMs: 1_700_000_000_000);

        // 重复调用不应抛出异常（PK 冲突时 ON CONFLICT DO NOTHING）
        await tombstoneStore.RecordDeletionAsync(
            userId,
            deletionEventId: "evt-idempotent-2",
            deletedAtMs: 1_700_000_000_001);

        // 状态应为 Deleting
        var state = await tombstoneStore.GetLifecycleStateAsync(userId);
        Assert.Equal(UserLifecycleState.Deleting, state);

        // 升级到 Deleted 也是幂等的
        await tombstoneStore.RecordDeletionCompletedAsync(userId);
        await tombstoneStore.RecordDeletionCompletedAsync(userId);

        state = await tombstoneStore.GetLifecycleStateAsync(userId);
        Assert.Equal(UserLifecycleState.Deleted, state);
    }

    [Fact]
    public async Task BatchGetUserLifecycleState_ReturnsActiveForUnknownUsers()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_batch_get");
        var tombstoneStore = new NpgsqlUserDeletionTombstoneStore(
            client,
            schema,
            NullLogger<NpgsqlUserDeletionTombstoneStore>.Instance);

        const long activeUser1 = 9_400_000_041L;
        const long activeUser2 = 9_400_000_042L;
        const long deletingUser = 9_400_000_043L;
        const long deletedUser = 9_400_000_044L;

        await tombstoneStore.RecordDeletionAsync(
            deletingUser,
            deletionEventId: "evt-batch-deleting",
            deletedAtMs: 1_700_000_000_000);

        await tombstoneStore.RecordDeletionAsync(
            deletedUser,
            deletionEventId: "evt-batch-deleted",
            deletedAtMs: 1_700_000_000_001);
        await tombstoneStore.RecordDeletionCompletedAsync(deletedUser);

        var states = await tombstoneStore.BatchGetUserLifecycleStateAsync(
            [activeUser1, activeUser2, deletingUser, deletedUser]);

        Assert.Equal(4, states.Count);
        Assert.Equal(UserLifecycleState.Active, states[activeUser1]);
        Assert.Equal(UserLifecycleState.Active, states[activeUser2]);
        Assert.Equal(UserLifecycleState.Deleting, states[deletingUser]);
        Assert.Equal(UserLifecycleState.Deleted, states[deletedUser]);
    }

    [Fact]
    public async Task ActiveUser_CanSendMessage_AfterConcurrentDeletionRollback()
    {
        var (client, schema) = await CreateStoreAsync("rt_lifecycle_active_after_rollback");
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            new PostgresConversationMessageMutationPolicy(
                NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance);

        const long senderUserId = 9_400_000_051L;
        const long receiverUserId = 9_400_000_052L;
        var lockKey = LifecycleNamespaceKey ^ senderUserId;

        // 连接 A：获取排他锁但随后回滚（模拟删除流程被中断）
        await using var connectionA = await client.GetDataSource().OpenConnectionAsync();
        await using var transactionA = await connectionA.BeginTransactionAsync();
        await AcquireExclusiveLockAsync(connectionA, transactionA, lockKey);

        // 释放排他锁
        await transactionA.RollbackAsync();

        // 用户仍为 Active（无 tombstone），SaveAsync 应成功
        var message = CreateDirectMessage("msg-after-rollback", senderUserId, receiverUserId);
        var evt = CreateDirectMessageEvent("evt-after-rollback", senderUserId, receiverUserId, message.MessageId);
        var result = await messageStore.SaveAsync(message, evt);
        Assert.Equal(RealtimeMessagePersistKind.Created, result.Kind);
    }

    [Fact]
    public async Task DirectAdmissionSequenceGate_ReplayAndAuthorizationReject_HaveNoSequenceSideEffects()
    {
        var (client, schema) = await CreateStoreAsync("rt_direct_admission_sequence_gate");
        await EnsureIdentitySchemaAsync(client);
        var ledger = new NpgsqlCommandIdempotencyLedger(
            client,
            schema,
            NullLogger<NpgsqlCommandIdempotencyLedger>.Instance);
        var messageStore = new NpgsqlRealtimeMessageStore(
            client,
            schema,
            new PostgresConversationMessageMutationPolicy(
                NullLogger<PostgresConversationMessageMutationPolicy>.Instance),
            NullLogger<NpgsqlRealtimeMessageStore>.Instance,
            idempotencyLedger: ledger);

        const long senderUserId = 9_400_000_061L;
        const long receiverUserId = 9_400_000_062L;
        await SeedDirectAuthorizationAsync(client, senderUserId, receiverUserId, allow: true);

        var message = CreateDirectMessage("msg-direct-gate-created", senderUserId, receiverUserId);
        var evt = CreateDirectMessageEvent(
            "evt-direct-gate-created",
            senderUserId,
            receiverUserId,
            message.MessageId);
        var created = await messageStore.SaveAsync(message, evt);
        var replay = await messageStore.SaveAsync(message, evt);

        Assert.Equal(RealtimeMessagePersistKind.Created, created.Kind);
        Assert.Equal(RealtimeMessagePersistKind.Duplicate, replay.Kind);

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        await using (var command = new NpgsqlCommand(
                         $"""
                          SELECT c.last_sequence, sender.sent_count,
                                 (SELECT COUNT(*) FROM {schema.MessagesTableSql}),
                                 (SELECT COUNT(*) FROM {schema.OutboxTableSql}),
                                 (SELECT COUNT(*) FROM {schema.CommandIdempotencyLedgerTableSql})
                          FROM {schema.ConversationsTableSql} AS c
                          INNER JOIN {schema.ConversationMembersTableSql} AS sender
                            ON sender.conversation_id = c.conversation_id
                           AND sender.user_id = @sender_user_id
                          WHERE c.conversation_id = @conversation_id;
                          """,
                         connection))
        {
            command.Parameters.AddWithValue("sender_user_id", senderUserId);
            command.Parameters.AddWithValue(
                "conversation_id",
                ConversationId.CreateDirect(senderUserId, receiverUserId));
            await using var reader = await command.ExecuteReaderAsync();
            Assert.True(await reader.ReadAsync());
            Assert.Equal(1L, reader.GetInt64(0));
            Assert.Equal(1L, reader.GetInt64(1));
            Assert.Equal(1L, reader.GetInt64(2));
            Assert.Equal(1L, reader.GetInt64(3));
            Assert.Equal(1L, reader.GetInt64(4));
        }

        const long deniedSenderUserId = 9_400_000_063L;
        const long deniedReceiverUserId = 9_400_000_064L;
        await SeedDirectAuthorizationAsync(
            client,
            deniedSenderUserId,
            deniedReceiverUserId,
            allow: false);
        var deniedMessage = CreateDirectMessage(
            "msg-direct-gate-denied",
            deniedSenderUserId,
            deniedReceiverUserId);
        var deniedEvent = CreateDirectMessageEvent(
            "evt-direct-gate-denied",
            deniedSenderUserId,
            deniedReceiverUserId,
            deniedMessage.MessageId);

        var denied = await messageStore.SaveAsync(deniedMessage, deniedEvent);

        Assert.Equal(RealtimeMessagePersistKind.NotAllowed, denied.Kind);
        Assert.Equal(DirectMessageAuthorizationDecision.NotFriend, denied.AuthorizationDecision);
        await using var verifyConnection = await client.GetDataSource().OpenConnectionAsync();
        await using var verifyCommand = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT COUNT(*) FROM {schema.ConversationsTableSql}
                  WHERE conversation_id = @conversation_id),
                 (SELECT COUNT(*) FROM {schema.MessagesTableSql}
                  WHERE message_id = @message_id),
                 (SELECT COUNT(*) FROM {schema.OutboxTableSql}
                  WHERE event_id = @event_id),
                 (SELECT COUNT(*) FROM {schema.CommandIdempotencyLedgerTableSql}
                  WHERE sender_user_id = @sender_user_id
                    AND client_message_id = @client_message_id);
             """,
            verifyConnection);
        verifyCommand.Parameters.AddWithValue(
            "conversation_id",
            ConversationId.CreateDirect(deniedSenderUserId, deniedReceiverUserId));
        verifyCommand.Parameters.AddWithValue("message_id", deniedMessage.MessageId);
        verifyCommand.Parameters.AddWithValue("event_id", deniedEvent.EventId);
        verifyCommand.Parameters.AddWithValue("sender_user_id", deniedSenderUserId);
        verifyCommand.Parameters.AddWithValue("client_message_id", deniedMessage.ClientMessageId);
        await using var verifyReader = await verifyCommand.ExecuteReaderAsync();
        Assert.True(await verifyReader.ReadAsync());
        Assert.Equal(0L, verifyReader.GetInt64(0));
        Assert.Equal(0L, verifyReader.GetInt64(1));
        Assert.Equal(0L, verifyReader.GetInt64(2));
        Assert.Equal(0L, verifyReader.GetInt64(3));
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateStoreAsync(
        string schemaName)
    {
        var connectionString = PostgresConnectionString;
        var schema = new RealtimeDatabaseSchema($"{schemaName}_{_schemaSuffix}");
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }

    private static async Task EnsureIdentitySchemaAsync(RealtimeDatabaseClient client)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            CREATE TABLE IF NOT EXISTS public."AspNetUsers"
            (
                "Id" bigint PRIMARY KEY,
                "FriendRequestPolicy" smallint NOT NULL DEFAULT 1
            );
            CREATE TABLE IF NOT EXISTS public."T_BlockRecords"
            (
                "BlockerId" bigint NOT NULL,
                "BlockedUserId" bigint NOT NULL,
                PRIMARY KEY ("BlockerId", "BlockedUserId")
            );
            CREATE TABLE IF NOT EXISTS public."T_UserFriendEntry"
            (
                "UserId" bigint NOT NULL,
                "FriendId" bigint NOT NULL,
                "IsDeleted" boolean NOT NULL DEFAULT FALSE,
                PRIMARY KEY ("UserId", "FriendId")
            );
            """,
            connection);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task SeedDirectAuthorizationAsync(
        RealtimeDatabaseClient client,
        long senderUserId,
        long receiverUserId,
        bool allow)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var command = new NpgsqlCommand(
            """
            INSERT INTO public."AspNetUsers" ("Id")
            VALUES (@sender_user_id), (@receiver_user_id)
            ON CONFLICT ("Id") DO NOTHING;

            INSERT INTO public."T_UserFriendEntry" ("UserId", "FriendId", "IsDeleted")
            SELECT item.user_id, item.friend_id, FALSE
            FROM (VALUES
                (@sender_user_id, @receiver_user_id),
                (@receiver_user_id, @sender_user_id)
            ) AS item(user_id, friend_id)
            WHERE @allow
            ON CONFLICT ("UserId", "FriendId")
            DO UPDATE SET "IsDeleted" = FALSE;
            """,
            connection);
        command.Parameters.AddWithValue("sender_user_id", senderUserId);
        command.Parameters.AddWithValue("receiver_user_id", receiverUserId);
        command.Parameters.AddWithValue("allow", allow);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task AcquireExclusiveLockAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long lockKey)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock(@key);",
            connection,
            transaction);
        cmd.Parameters.AddWithValue("key", lockKey);
        await cmd.ExecuteNonQueryAsync();
    }

    private static async Task AcquireSharedLockAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long lockKey,
        CancellationToken ct = default)
    {
        await using var cmd = new NpgsqlCommand(
            "SELECT pg_advisory_xact_lock_shared(@key);",
            connection,
            transaction);
        cmd.Parameters.AddWithValue("key", lockKey);
        await cmd.ExecuteNonQueryAsync(ct);
    }

    private static RealtimeMessageRecord CreateDirectMessage(
        string messageId,
        long sender,
        long receiver) =>
        new()
        {
            MessageId = messageId,
            ClientMessageId = $"client-{messageId}",
            SenderUserId = sender,
            SenderSessionId = "s1",
            ReceiverUserId = receiver,
            ConversationId = ConversationId.CreateDirect(sender, receiver),
            Content = "lifecycle-test",
            ReceivedAtMs = 1_700_000_000_500
        };

    private static RealtimeEvent CreateDirectMessageEvent(
        string eventId,
        long sender,
        long receiver,
        string messageId)
    {
        var conversationId = ConversationId.CreateDirect(sender, receiver);
        return new RealtimeEvent
        {
            EventId = eventId,
            Type = RealtimeEventType.MessageReceived,
            TargetUserId = receiver,
            ActorUserId = sender,
            MessageId = messageId,
            SessionId = "s1",
            PayloadJson = JsonSerializer.Serialize(
                new RealtimeChatMessagePayload
                {
                    MessageId = messageId,
                    ClientMessageId = $"client-{messageId}",
                    SenderUserId = sender,
                    SenderSessionId = "s1",
                    ReceiverUserId = receiver,
                    ConversationId = conversationId,
                    Content = "lifecycle-test",
                    ReceivedAtMs = 1_700_000_000_500
                },
                RealtimeJsonSerializerContext.Default.RealtimeChatMessagePayload),
            OccurredAtMs = 1_700_000_000_500
        };
    }
}

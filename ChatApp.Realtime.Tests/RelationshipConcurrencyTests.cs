using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Relationships;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P1-5：关系域并发状态机测试。
/// <para>
/// 权威状态由 PostgreSQL 事务与不可变幂等 Ledger（relationship_mutation_requests）
/// 决定。以下用例验证并发下的状态机不变量：
/// 双方同时发好友请求、接受与拒绝并发、删除好友与重新建交并发、
/// 拉黑与接受好友并发、重复 RequestId 跨 Gateway 幂等、已注销用户拒绝变更。
/// </para>
/// </summary>
public sealed class RelationshipConcurrencyTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task BothSendFriendRequest_Concurrent_NoDuplicateFriendship()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_both_send");
        var store = CreateRelationshipStore(client, schema);

        var aReq = "req-a-to-b";
        var bReq = "req-b-to-a";
        var (aResult, bResult) = await RunConcurrently(
            store.SendFriendRequestAsync(aReq, 10, 20, "hi", "sess", Now(), default),
            store.SendFriendRequestAsync(bReq, 20, 10, "yo", "sess", Now(), default));

        Assert.All(new[] { aResult, bResult }, r =>
            Assert.True(r.Succeeded || r.ErrorCode == "pending_request_exists", r.ErrorCode));

        Assert.Equal(0, await CountRowsAsync(client, schema, "friendships", "1=1"));

        var pending = await CountRowsAsync(
            client, schema, "friend_requests", "\"status\" = 0");
        Assert.True(pending >= 1, $"期望至少 1 条 Pending 请求，实际 {pending}。");

        var forward = await CountRowsAsync(
            client, schema, "friend_requests",
            "\"requester_id\"=10 AND \"target_id\"=20 AND \"status\"=0");
        var backward = await CountRowsAsync(
            client, schema, "friend_requests",
            "\"requester_id\"=20 AND \"target_id\"=10 AND \"status\"=0");
        Assert.True(forward <= 1, "A→B 不应出现重复 Pending 请求。");
        Assert.True(backward <= 1, "B→A 不应出现重复 Pending 请求。");
    }

    [Fact]
    public async Task AcceptAndDecline_Concurrent_ExactlyOneWins()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_accept_decline");
        var store = CreateRelationshipStore(client, schema);

        var send = await store.SendFriendRequestAsync(
            "req-1", 10, 20, "hi", "sess", Now(), default);
        Assert.True(send.Succeeded);
        var requestId = send.ResourceId!;

        var (accepted, declined) = await RunConcurrently(
            store.AcceptFriendRequestAsync("acc-1", 20, requestId, "sess", Now(), default),
            store.DeclineFriendRequestAsync("dec-1", 20, requestId, "sess", Now(), default));

        var successCount = (accepted.Succeeded ? 1 : 0) + (declined.Succeeded ? 1 : 0);
        Assert.Equal(1, successCount);

        var loser = accepted.Succeeded ? declined : accepted;
        Assert.Equal("request_not_pending", loser.ErrorCode);

        var friendshipCount = await CountRowsAsync(
            client, schema, "friendships",
            "\"user_id_low\"=10 AND \"user_id_high\"=20");
        var status = await ReadScalarAsync(
            client, schema, "friend_requests", "\"status\"", "\"request_id\"=@id",
            new NpgsqlParameter("id", requestId));

        if (accepted.Succeeded)
        {
            Assert.Equal(1, friendshipCount);
            Assert.Equal((short)1, (short)status!); // Accepted
        }
        else
        {
            Assert.Equal(0, friendshipCount);
            Assert.Equal((short)2, (short)status!); // Declined
        }
    }

    [Fact]
    public async Task RemoveFriend_And_Resend_Concurrent_NoCorruptState()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_remove_resend");
        var store = CreateRelationshipStore(client, schema);

        var send = await store.SendFriendRequestAsync(
            "req-1", 10, 20, "hi", "sess", Now(), default);
        var accept = await store.AcceptFriendRequestAsync(
            "acc-1", 20, send.ResourceId!, "sess", Now(), default);
        Assert.True(accept.Succeeded);

        var (removed, resent) = await RunConcurrently(
            store.RemoveFriendAsync("rm-1", 10, 20, "sess", Now(), default),
            store.SendFriendRequestAsync("req-2", 10, 20, "again", "sess", Now(), default));

        Assert.True(removed.Succeeded, "删除好友应当成功（友谊存在）。");

        Assert.Equal(0, await CountRowsAsync(
            client, schema, "friendships", "\"user_id_low\"=10 AND \"user_id_high\"=20"));

        if (resent.Succeeded)
        {
            Assert.Equal(1, await CountRowsAsync(
                client, schema, "friend_requests",
                "\"requester_id\"=10 AND \"target_id\"=20 AND \"status\"=0"));
        }
        else if (resent.ErrorCode == "already_friends")
        {
            Assert.Equal(0, await CountRowsAsync(
                client, schema, "friend_requests",
                "\"requester_id\"=10 AND \"target_id\"=20 AND \"status\"=0"));
        }
        else
        {
            Assert.Fail($"未预期的错误码：{resent.ErrorCode}");
        }
    }

    [Fact]
    public async Task BlockAndAccept_Concurrent_BlockAndFriendshipBothPersist()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_block_accept");
        var store = CreateRelationshipStore(client, schema);

        var send = await store.SendFriendRequestAsync(
            "req-1", 10, 20, "hi", "sess", Now(), default);
        Assert.True(send.Succeeded);
        var requestId = send.ResourceId!;

        var (accepted, blocked) = await RunConcurrently(
            store.AcceptFriendRequestAsync("acc-1", 20, requestId, "sess", Now(), default),
            store.BlockUserAsync("blk-1", 10, 20, "sess", Now(), default));

        Assert.True(accepted.Succeeded, "接受好友请求应成功。");
        Assert.True(blocked.Succeeded, "拉黑应成功。");

        Assert.Equal(1, await CountRowsAsync(
            client, schema, "friendships", "\"user_id_low\"=10 AND \"user_id_high\"=20"));
        Assert.Equal(1, await CountInPublicTableAsync(
            client, "public", "T_BlockRecords", "\"BlockerId\"=10 AND \"BlockedUserId\"=20"));
    }

    [Fact]
    public async Task DuplicateRequestId_AcrossGateways_IsIdempotent()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_idempotent");
        var storeA = CreateRelationshipStore(client, schema);
        var storeB = CreateRelationshipStore(client, schema);

        var first = await storeA.SendFriendRequestAsync(
            "dup-1", 10, 20, "hi", "sess-a", Now(), default);
        Assert.True(first.Succeeded);

        var second = await storeB.SendFriendRequestAsync(
            "dup-1", 10, 20, "hi", "sess-b", Now(), default);
        Assert.True(second.Succeeded);
        Assert.Equal(first.ResourceId, second.ResourceId);

        Assert.Equal(1, await CountRowsAsync(
            client, schema, "friend_requests", "\"request_id\"='dup-1'"));
        Assert.Equal(1, await CountRowsAsync(
            client, schema, "relationship_change_log",
            "\"request_id\"='dup-1' AND \"user_id\"=20 AND \"list_type\"=" +
            (short)RelationshipListType.FriendRequests));
    }

    [Fact]
    public async Task Processor_RejectsMutation_ForDeletedUser()
    {
        var (client, schema) = await CreateDatabaseAsync("rel_deleted_user");
        var store = new RecordingRelationshipStore();
        var processor = new DefaultRelationshipCommandProcessor(
            store, new DeletedUserTombstone());

        var result = await processor.ProcessAsync(new RelationshipCommand
        {
            RequestId = "req-1",
            ActorUserId = 10,
            Operation = RelationshipOperation.SendFriendRequest,
            TargetUserId = 20,
            Message = "hi",
            ActorSessionId = "sess"
        });

        Assert.False(result.Succeeded);
        Assert.Equal("user_deleted", result.ErrorCode);
        Assert.Equal(0, store.MutationCount);
    }

    private static async Task<(T A, T B)> RunConcurrently<T>(Task<T> a, Task<T> b)
    {
        var both = await Task.WhenAll(a, b);
        return (both[0], both[1]);
    }

    private static long Now() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string RealtimeTable(RealtimeDatabaseSchema schema, string table) =>
        table switch
        {
            "friendships" => schema.FriendshipsTableSql,
            "friend_requests" => schema.FriendRequestsTableSql,
            "relationship_change_log" => schema.RelationshipChangeLogTableSql,
            _ => throw new InvalidOperationException($"Unknown realtime table {table}")
        };

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateDatabaseAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(connectionString, NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance).MigrateAsync(connection);

        // 黑名单复用 public."T_BlockRecords"（由主库 schema 迁移创建，realtime schema 迁移不包含）。
        await using (var block = new NpgsqlCommand(
            """CREATE TABLE IF NOT EXISTS public."T_BlockRecords" ("BlockerId" bigint NOT NULL, "BlockedUserId" bigint NOT NULL, PRIMARY KEY ("BlockerId", "BlockedUserId"))""",
            connection))
        {
            await block.ExecuteNonQueryAsync();
        }

        return (client, schema);
    }

    private static NpgsqlRelationshipStore CreateRelationshipStore(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema) =>
        new(client, schema);

    private static async Task<int> CountRowsAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string table,
        string where)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {RealtimeTable(schema, table)} WHERE {where}", connection);
        return (int)(long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<int> CountInPublicTableAsync(
        RealtimeDatabaseClient client,
        string schemaName,
        string table,
        string where)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schemaName}.\"{table}\" WHERE {where}", connection);
        return (int)(long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<object?> ReadScalarAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        string realtimeTable,
        string column,
        string where,
        NpgsqlParameter parameter)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT {column} FROM {RealtimeTable(schema, realtimeTable)} WHERE {where}", connection);
        cmd.Parameters.Add(parameter);
        return await cmd.ExecuteScalarAsync();
    }

    private sealed class RecordingRelationshipStore : IRelationshipStore
    {
        public int MutationCount { get; private set; }

        public Task<RelationshipMutatePersistResult> SendFriendRequestAsync(
            string requestId, long actorUserId, long targetUserId, string? message,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default)
        {
            MutationCount++;
            return Task.FromResult(RelationshipMutatePersistResult.Ok(requestId, targetUserId));
        }

        public Task<RelationshipMutatePersistResult> AcceptFriendRequestAsync(
            string requestId, long actorUserId, string requestIdToRespond,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
            ThrowNotSupported();

        public Task<RelationshipMutatePersistResult> DeclineFriendRequestAsync(
            string requestId, long actorUserId, string requestIdToRespond,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
            ThrowNotSupported();

        public Task<RelationshipMutatePersistResult> RemoveFriendAsync(
            string requestId, long actorUserId, long targetUserId,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
            ThrowNotSupported();

        public Task<RelationshipMutatePersistResult> BlockUserAsync(
            string requestId, long actorUserId, long targetUserId,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
            ThrowNotSupported();

        public Task<RelationshipMutatePersistResult> UnblockUserAsync(
            string requestId, long actorUserId, long targetUserId,
            string? actorSessionId, long occurredAtMs, CancellationToken ct = default) =>
            ThrowNotSupported();

        public Task<IReadOnlyList<RelationshipListItem>> ListFriendsAsync(
            long actorUserId, int? pageSize, string? cursor,
            long afterChangedAtMs = 0, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

        public Task<IReadOnlyList<RelationshipListItem>> ListFriendRequestsAsync(
            long actorUserId, int? pageSize, string? cursor,
            long afterChangedAtMs = 0, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

        public Task<IReadOnlyList<RelationshipListItem>> ListBlockedUsersAsync(
            long actorUserId, int? pageSize, string? cursor,
            long afterChangedAtMs = 0, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RelationshipListItem>>(Array.Empty<RelationshipListItem>());

        public Task<IReadOnlyList<RelationshipChangeLogEntry>> ListChangesAsync(
            long userId, RelationshipListType listType, long afterSequence, int limit, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyList<RelationshipChangeLogEntry>>(Array.Empty<RelationshipChangeLogEntry>());

        public Task<long> GetRelationshipRetentionFloorAsync(
            long userId, RelationshipListType listType, CancellationToken ct = default) =>
            Task.FromResult(0L);

        private static Task<RelationshipMutatePersistResult> ThrowNotSupported() =>
            throw new NotSupportedException();
    }

    private sealed class DeletedUserTombstone : IUserDeletionTombstoneStore
    {
        public Task<bool> IsUserDeletedAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(true);

        public Task<UserLifecycleState> GetLifecycleStateAsync(long userId, CancellationToken ct = default) =>
            Task.FromResult(UserLifecycleState.Deleted);

        public Task<IReadOnlyDictionary<long, UserLifecycleState>> BatchGetUserLifecycleStateAsync(
            IReadOnlyList<long> userIds, CancellationToken ct = default) =>
            Task.FromResult<IReadOnlyDictionary<long, UserLifecycleState>>(
                userIds.ToDictionary(id => id, _ => UserLifecycleState.Deleted));

        public Task RecordDeletionAsync(long userId, string deletionEventId, long deletedAtMs, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task RecordDeletionCompletedAsync(long userId, CancellationToken ct = default) =>
            Task.CompletedTask;

        public Task<long> PurgeOlderThanAsync(long cutoffMs, int batchSize, CancellationToken ct = default) =>
            Task.FromResult(0L);
    }
}
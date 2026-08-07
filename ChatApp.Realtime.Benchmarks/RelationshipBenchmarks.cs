using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 门禁4：Relationship 域 SQL 命令数上限断言。
/// <para>
/// 每个 mutation 走「幂等读 → 事务内多步 → 变更日志 → Outbox 批量 → 幂等写」，
/// 期望 SQL 次数固定。若次数超过上限说明引入了额外往返（回退、N+1、逐行 Outbox 等），
/// 基准会抛异常使门禁失败。
/// </para>
/// <para>
/// 期望 SQL 次数（含幂等读）：
/// <list type="bullet">
/// <item>SendFriendRequest = 1 幂等读 + 1 checkFriend + 1 checkPending + 1 insert + 1 changelog + 1 outbox + 1 幂等写 = 7</item>
/// <item>AcceptFriendRequest = 1 + 1 FOR UPDATE + 1 update + 1 insertFriendship + 3 changelog + 1 outbox + 1 幂等写 = 9</item>
/// <item>DeclineFriendRequest = 1 + 1 FOR UPDATE + 1 update + 1 changelog + 1 outbox + 1 幂等写 = 6</item>
/// <item>RemoveFriend = 1 + 1 select + 1 delete + 2 changelog + 1 outbox + 1 幂等写 = 7</item>
/// <item>BlockUser = 1 + 1 insert + 1 changelog + 1 outbox + 1 幂等写 = 5</item>
/// <item>UnblockUser = 1 + 1 delete + 1 changelog + 1 outbox + 1 幂等写 = 5</item>
/// <item>List* / ListChanges / RetentionFloor = 1</item>
/// </list>
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class RelationshipBenchmarks
{
    private const int SqlSendFriendRequest = 7;
    private const int SqlAcceptFriendRequest = 9;
    private const int SqlDeclineFriendRequest = 6;
    private const int SqlRemoveFriend = 7;
    private const int SqlBlockUser = 5;
    private const int SqlUnblockUser = 5;
    private const int SqlSingleSelect = 1;

    private const long ActorUserId = 1001;
    private const long RequesterUserId = 1002;
    private const long FriendUserId = 1003;
    private const long BlockedUserId = 1004;
    private const long NewTargetUserId = 1005;
    private const long NewBlockUserId = 1006;

    private const string PendingRequestId = "bench-pending-req";

    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlConnection? _connection;
    private NpgsqlRelationshipStore? _store;

    [GlobalSetup]
    public void Initialize()
    {
        InitializeAsync().GetAwaiter().GetResult();
    }

    private async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);
        _schema = new RealtimeDatabaseSchema("realtime_bench_rel");

        await using var migrateConnection = await _dbClient.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        _store = new NpgsqlRelationshipStore(_dbClient, _schema);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        CleanupAsync().GetAwaiter().GetResult();
    }

    private async Task CleanupAsync()
    {
        if (_connection is not null)
            await _connection.DisposeAsync();
        if (_dbClient is not null)
            await _dbClient.DisposeAsync();
        if (_container is not null)
            await _container.DisposeAsync();
    }

    /// <summary>每次迭代前清空关系表 + outbox，并重植各类前置数据。</summary>
    [IterationSetup]
    public void SetupIteration()
    {
        SetupIterationCore().GetAwaiter().GetResult();
    }

    private async Task SetupIterationCore()
    {
        var schema = _schema!;
        await using var truncate = new NpgsqlCommand(
            $"""
             TRUNCATE {schema.RelationshipChangeLogTableSql};
             TRUNCATE {schema.FriendRequestsTableSql};
             TRUNCATE {schema.FriendshipsTableSql};
             TRUNCATE {schema.RelationshipMutationRequestsTableSql};
             TRUNCATE {schema.OutboxTableSql};
             TRUNCATE public."T_BlockRecords";
             """,
            _connection!);
        await truncate.ExecuteNonQueryAsync();

        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // 待处理好友请求：requester → target(Actor)
        await using (var pending = new NpgsqlCommand(
                         $"""
                          INSERT INTO {schema.FriendRequestsTableSql}
                              ("request_id", "requester_id", "target_id", "message", "status", "created_at_ms")
                          VALUES (@req, @requester, @target, 'hi', 0, @now)
                          ON CONFLICT DO NOTHING;
                          """,
                         _connection))
        {
            pending.Parameters.AddWithValue("req", PendingRequestId);
            pending.Parameters.AddWithValue("requester", RequesterUserId);
            pending.Parameters.AddWithValue("target", ActorUserId);
            pending.Parameters.AddWithValue("now", nowMs);
            await pending.ExecuteNonQueryAsync();
        }

        // 既有友谊：Actor ↔ Friend
        await using (var friendship = new NpgsqlCommand(
                         $"""
                          INSERT INTO {schema.FriendshipsTableSql}
                              ("friendship_id", "user_id_low", "user_id_high", "created_at_ms")
                          VALUES ('bench-fid', @low, @high, @now)
                          ON CONFLICT DO NOTHING;
                          """,
                         _connection))
        {
            friendship.Parameters.AddWithValue("low", Math.Min(ActorUserId, FriendUserId));
            friendship.Parameters.AddWithValue("high", Math.Max(ActorUserId, FriendUserId));
            friendship.Parameters.AddWithValue("now", nowMs);
            await friendship.ExecuteNonQueryAsync();
        }

        // 既有拉黑：Actor 屏蔽 BlockedUser
        await using (var block = new NpgsqlCommand(
                         """INSERT INTO public."T_BlockRecords" ("BlockerId", "BlockedUserId") VALUES (@b, @t) ON CONFLICT DO NOTHING;""",
                         _connection))
        {
            block.Parameters.AddWithValue("b", ActorUserId);
            block.Parameters.AddWithValue("t", BlockedUserId);
            await block.ExecuteNonQueryAsync();
        }
    }

    [Benchmark(Description = "Relationship SendFriendRequest (7 SQL)")]
    public async Task<int> SendFriendRequest()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.SendFriendRequestAsync(
            requestId: "bench-send",
            actorUserId: ActorUserId,
            targetUserId: NewTargetUserId,
            message: "hello",
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlSendFriendRequest);
    }

    [Benchmark(Description = "Relationship AcceptFriendRequest (9 SQL)")]
    public async Task<int> AcceptFriendRequest()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.AcceptFriendRequestAsync(
            requestId: "bench-accept",
            actorUserId: ActorUserId,
            requestIdToRespond: PendingRequestId,
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlAcceptFriendRequest);
    }

    [Benchmark(Description = "Relationship DeclineFriendRequest (6 SQL)")]
    public async Task<int> DeclineFriendRequest()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.DeclineFriendRequestAsync(
            requestId: "bench-decline",
            actorUserId: ActorUserId,
            requestIdToRespond: PendingRequestId,
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlDeclineFriendRequest);
    }

    [Benchmark(Description = "Relationship RemoveFriend (7 SQL)")]
    public async Task<int> RemoveFriend()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.RemoveFriendAsync(
            requestId: "bench-remove",
            actorUserId: ActorUserId,
            targetUserId: FriendUserId,
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlRemoveFriend);
    }

    [Benchmark(Description = "Relationship BlockUser (5 SQL)")]
    public async Task<int> BlockUser()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.BlockUserAsync(
            requestId: "bench-block",
            actorUserId: ActorUserId,
            targetUserId: NewBlockUserId,
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlBlockUser);
    }

    [Benchmark(Description = "Relationship UnblockUser (5 SQL)")]
    public async Task<int> UnblockUser()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.UnblockUserAsync(
            requestId: "bench-unblock",
            actorUserId: ActorUserId,
            targetUserId: BlockedUserId,
            actorSessionId: null,
            occurredAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        return AssertSqlCount(SqlUnblockUser);
    }

    [Benchmark(Description = "Relationship ListFriends (1 SQL)")]
    public async Task<int> ListFriends()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.ListFriendsAsync(ActorUserId, pageSize: 50, cursor: null);
        return AssertSqlCount(SqlSingleSelect);
    }

    [Benchmark(Description = "Relationship ListFriendRequests (1 SQL)")]
    public async Task<int> ListFriendRequests()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.ListFriendRequestsAsync(ActorUserId, pageSize: 50, cursor: null);
        return AssertSqlCount(SqlSingleSelect);
    }

    [Benchmark(Description = "Relationship ListBlockedUsers (1 SQL)")]
    public async Task<int> ListBlockedUsers()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.ListBlockedUsersAsync(ActorUserId, pageSize: 50, cursor: null);
        return AssertSqlCount(SqlSingleSelect);
    }

    [Benchmark(Description = "Relationship ListChanges ChangeLog index scan (1 SQL)")]
    public async Task<int> ListChanges()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.ListChangesAsync(ActorUserId, RelationshipListType.FriendRequests, afterSequence: 0, limit: 51);
        return AssertSqlCount(SqlSingleSelect);
    }

    [Benchmark(Description = "Relationship GetRelationshipRetentionFloor (1 SQL)")]
    public async Task<int> GetRelationshipRetentionFloor()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _store!.GetRelationshipRetentionFloorAsync(ActorUserId, RelationshipListType.FriendRequests);
        return AssertSqlCount(SqlSingleSelect);
    }

    private int AssertSqlCount(int upperBound)
    {
        var count = NpgsqlSqlCommandCounter.GetCommandCount();
        if (count > upperBound)
        {
            throw new InvalidOperationException(
                $"SQL 命令数 {count} 超过门禁上限 {upperBound}。可能引入了额外往返或 N+1。");
        }
        return count;
    }
}
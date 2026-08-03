using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 五-4：授权策略链 SQL 命令数基线。
/// <para>
/// 验证新增的 5 个授权 Store 的 SQL 命令数门禁：
/// <list type="bullet">
/// <item>UserExistenceChecker.ExistsAsync → 1 次 SQL</item>
/// <item>UserExistenceChecker.FilterNonExistentAsync → 1 次 SQL（批量）</item>
/// <item>BlockListStore.IsBlockedAsync → 1 次 SQL</item>
/// <item>DirectMessagePolicy.CheckAsync → 1 次 SQL</item>
/// <item>PrivacySettingStore.AllowsDirectMessageAsync → 1 次 SQL</item>
/// <item>完整链（5 步顺序）→ 5 次 SQL</item>
/// </list>
/// </para>
/// <para>
/// 合成数据：在 Testcontainers PG 中创建测试用户、屏蔽记录、好友关系。
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class AuthorizationChainBenchmarks : IAsyncLifetime
{
    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private NpgsqlUserExistenceChecker? _existenceChecker;
    private NpgsqlBlockListStore? _blockListStore;
    private NpgsqlDirectMessagePolicy? _dmPolicy;
    private NpgsqlPrivacySettingStore? _privacyStore;
    private NpgsqlMessageRateLimiter? _rateLimiter;

    private const long SenderUserId = 10001;
    private const long ReceiverUserId = 10002;
    private const long BlockedUserId = 10003;
    private const long NonExistentUser = 99999;

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(_container.GetConnectionString(), NullLogger<RealtimeDatabaseClient>.Instance);
        await SetupSchemaAsync();
        await SeedDataAsync();

        _existenceChecker = new NpgsqlUserExistenceChecker(_dbClient);
        _blockListStore = new NpgsqlBlockListStore(_dbClient);
        _dmPolicy = new NpgsqlDirectMessagePolicy(_dbClient);
        _privacyStore = new NpgsqlPrivacySettingStore(_dbClient);
        _rateLimiter = new NpgsqlMessageRateLimiter();
    }

    public async Task DisposeAsync()
    {
        _rateLimiter?.Dispose();
        if (_container is not null)
            await _container.DisposeAsync();
    }

    [Benchmark]
    public async Task<int> UserExistenceCheck()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _existenceChecker!.ExistsAsync(SenderUserId);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> UserExistenceFilterBatch()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        var userIds = new long[] { SenderUserId, ReceiverUserId, NonExistentUser, 10004, 10005 };
        await _existenceChecker!.FilterNonExistentAsync(userIds);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> BlockListCheck()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _blockListStore!.IsBlockedAsync(ReceiverUserId, BlockedUserId);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> DirectMessagePolicyCheck()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _dmPolicy!.CheckAsync(SenderUserId, ReceiverUserId);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> PrivacySettingCheck()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _privacyStore!.AllowsDirectMessageAsync(ReceiverUserId, SenderUserId);
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    [Benchmark]
    public async Task<int> FullAuthorizationChain()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();

        // 1. User existence (sender + receiver)
        await _existenceChecker!.ExistsAsync(SenderUserId);
        await _existenceChecker!.ExistsAsync(ReceiverUserId);

        // 2. Block check
        await _blockListStore!.IsBlockedAsync(ReceiverUserId, SenderUserId);

        // 3. Privacy setting
        await _privacyStore!.AllowsDirectMessageAsync(ReceiverUserId, SenderUserId);

        // 4. DM policy
        await _dmPolicy!.CheckAsync(SenderUserId, ReceiverUserId);

        // 5. Rate limit
        await _rateLimiter!.TryAcquireAsync(SenderUserId);

        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    private async Task SetupSchemaAsync()
    {
        await using var conn = await _dbClient!.GetDataSource().OpenConnectionAsync();
        await using var cmd = new NpgsqlCommand("""
            CREATE TABLE IF NOT EXISTS public."AspNetUsers" (
                "Id" bigint PRIMARY KEY,
                "UserName" text,
                "LockoutEnabled" boolean NOT NULL DEFAULT false,
                "LockoutEnd" timestamp with time zone,
                "FriendRequestPolicy" smallint NOT NULL DEFAULT 1
            );

            CREATE TABLE IF NOT EXISTS public."T_BlockRecords" (
                "BlockerId" bigint NOT NULL,
                "BlockedUserId" bigint NOT NULL,
                PRIMARY KEY ("BlockerId", "BlockedUserId")
            );

            CREATE TABLE IF NOT EXISTS public."T_UserFriendEntry" (
                "UserId" bigint NOT NULL,
                "FriendId" bigint NOT NULL,
                "IsDeleted" boolean NOT NULL DEFAULT false,
                PRIMARY KEY ("UserId", "FriendId")
            );
            """, conn);
        await cmd.ExecuteNonQueryAsync();
    }

    private async Task SeedDataAsync()
    {
        await using var conn = await _dbClient!.GetDataSource().OpenConnectionAsync();

        // 创建测试用户
        await using var userCmd = new NpgsqlCommand("""
            INSERT INTO public."AspNetUsers" ("Id", "UserName", "LockoutEnabled", "LockoutEnd", "FriendRequestPolicy")
            VALUES
                (@sender, 'sender', false, NULL, 1),
                (@receiver, 'receiver', false, NULL, 1),
                (@blocked, 'blocked', false, NULL, 1)
            ON CONFLICT ("Id") DO NOTHING;
            """, conn);
        userCmd.Parameters.AddWithValue("sender", SenderUserId);
        userCmd.Parameters.AddWithValue("receiver", ReceiverUserId);
        userCmd.Parameters.AddWithValue("blocked", BlockedUserId);
        await userCmd.ExecuteNonQueryAsync();

        // 创建双向好友关系
        await using var friendCmd = new NpgsqlCommand("""
            INSERT INTO public."T_UserFriendEntry" ("UserId", "FriendId", "IsDeleted")
            VALUES
                (@sender, @receiver, false),
                (@receiver, @sender, false)
            ON CONFLICT DO NOTHING;
            """, conn);
        friendCmd.Parameters.AddWithValue("sender", SenderUserId);
        friendCmd.Parameters.AddWithValue("receiver", ReceiverUserId);
        await friendCmd.ExecuteNonQueryAsync();

        // 创建屏蔽关系：receiver 屏蔽 blocked
        await using var blockCmd = new NpgsqlCommand("""
            INSERT INTO public."T_BlockRecords" ("BlockerId", "BlockedUserId")
            VALUES (@receiver, @blocked)
            ON CONFLICT DO NOTHING;
            """, conn);
        blockCmd.Parameters.AddWithValue("receiver", ReceiverUserId);
        blockCmd.Parameters.AddWithValue("blocked", BlockedUserId);
        await blockCmd.ExecuteNonQueryAsync();
    }
}

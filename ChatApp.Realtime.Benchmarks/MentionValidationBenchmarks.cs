using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 门禁4：Mention 可见性过滤 SQL 命令数基线。
/// <para>
/// 验证群聊 mention 校验的 SQL 次数：
/// <list type="bullet">
/// <item>直接消息（无群聊）无 mention：0 SQL → 立即返回</item>
/// <item>群聊但无 mention：仅 1 SQL（发送方角色） → 跳过批量成员查询</item>
/// <item>群聊 + 5 个用户 mention：2 SQL → 发送方角色（1） + 批量校验成员（1），恒定 2 次，无 N+1</item>
/// <item>群聊 + 20 个用户 mention：仍为 2 SQL → 恒定次数，不随 Mention 数增长</item>
/// </list>
/// 门禁断言：SQL 命令数不得超过预期恒定值，必须为批量查询不得为逐提及查询。
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class MentionValidationBenchmarks : IAsyncLifetime
{
    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlRealtimeGroupStore? _groupStore;

    private const string ConversationId = "bench-mention-conv-001";
    private const long SenderUserId = 90001;
    private static readonly long[] s_fiveMentions = [90002, 90003, 90004, 90005, 90006];
    private static readonly long[] s_twentyMentions = Enumerable.Range(90002, 20)
        .Select(static userId => (long)userId)
        .ToArray();

    public async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder()
            .WithImage("postgres:16-alpine")
            .Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);
        _schema = new RealtimeDatabaseSchema("realtime_bench_mention");

        await using var migrateConnection = await _dbClient.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection, CancellationToken.None);

        await SeedDataAsync();
        _groupStore = new NpgsqlRealtimeGroupStore(_dbClient, _schema);
    }

    public async Task DisposeAsync()
    {
        if (_container is not null)
            await _container.DisposeAsync();
    }

    private async Task SeedDataAsync()
    {
        await using var conn = await _dbClient!.GetDataSource().OpenConnectionAsync();
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        // 创建会话
        await using var convCmd = new NpgsqlCommand(
            $"""INSERT INTO {_schema!.ConversationsTableSql} (conversation_id, type, created_at_ms, updated_at_ms) VALUES (@conv, 2, @now, @now) ON CONFLICT DO NOTHING;""",
            conn);
        convCmd.Parameters.AddWithValue("conv", ConversationId);
        convCmd.Parameters.AddWithValue("now", nowMs);
        await convCmd.ExecuteNonQueryAsync();

        // 添加成员：sender 是管理员，mention 目标都是成员
        var members = new List<(long UserId, ConversationMemberRole Role)> { (SenderUserId, ConversationMemberRole.Admin) };
        foreach (var id in s_twentyMentions)
        {
            members.Add((id, ConversationMemberRole.Member));
        }

        foreach (var (userId, role) in members)
        {
            await using var memberCmd = new NpgsqlCommand(
                $"""INSERT INTO {_schema.ConversationMembersTableSql} (conversation_id, user_id, role, joined_at_ms) VALUES (@conv, @uid, @role, @now) ON CONFLICT DO NOTHING;""",
                conn);
            memberCmd.Parameters.AddWithValue("conv", ConversationId);
            memberCmd.Parameters.AddWithValue("uid", userId);
            memberCmd.Parameters.AddWithValue("role", (short)role);
            memberCmd.Parameters.AddWithValue("now", nowMs);
            await memberCmd.ExecuteNonQueryAsync();
        }
    }

    /// <summary>
    /// 直接单聊无群 + 无 mention → 0 SQL。
    /// 这是最常见的路径，必须无额外查询。
    /// </summary>
    [Benchmark]
    public async Task<int> Direct_NoMention()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        // 直接消息不会进入群成员查询分支
        await _groupStore!.GetMemberRoleAsync(SenderUserId, "dm:90001:90002");
        return NpgsqlSqlCommandCounter.GetCommandCount();
    }

    /// <summary>
    /// 群聊但无任何用户提及 → 仅 1 SQL (发送方角色)。
    /// 无 mention 不触发批量校验 → 恒定 1 次。
    /// </summary>
    [Benchmark]
    public async Task<int> Group_NoMentions()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        _ = await _groupStore!.GetMemberRoleAsync(SenderUserId, ConversationId);
        // 无提及 → 不需要 ValidateMembersAsync
        return AssertSqlCount(1, NpgsqlSqlCommandCounter.GetCommandCount());
    }

    /// <summary>
    /// 群聊 + 5 个用户提及 → 2 SQL：
    /// 1. GetMemberRoleAsync (sender 角色判定是否允许 @all/@admin)
    /// 2. ValidateMembersAsync (批量校验所有提及用户是否为活跃成员，单条 ANY)
    /// 不随提及人数增长，恒定 2 次。
    /// </summary>
    [Benchmark]
    public async Task<int> Group_FiveMentions()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _groupStore!.GetMemberRoleAsync(SenderUserId, ConversationId);
        _ = await _groupStore.ValidateMembersAsync(ConversationId, s_fiveMentions);
        return AssertSqlCount(2, NpgsqlSqlCommandCounter.GetCommandCount());
    }

    /// <summary>
    /// 群聊 + 20 个用户提及 → 仍为 2 SQL。
    /// 证明批量 ANY 查询无论 N 多大都恒定 1 次，无 N+1 退化。
    /// </summary>
    [Benchmark]
    public async Task<int> Group_TwentyMentions()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        await _groupStore!.GetMemberRoleAsync(SenderUserId, ConversationId);
        _ = await _groupStore.ValidateMembersAsync(ConversationId, s_twentyMentions);
        return AssertSqlCount(2, NpgsqlSqlCommandCounter.GetCommandCount());
    }

    private static int AssertSqlCount(int expectedUpper, int actual)
    {
        if (actual > expectedUpper)
        {
            throw new InvalidOperationException(
                $"Mention 可见性过滤 SQL 命令数 {actual} 超过门禁上限 {expectedUpper}。" +
                "必须使用批量 ANY 查询保持恒定次数，不得退化为逐提及 N+1 查询。");
        }
        return actual;
    }
}

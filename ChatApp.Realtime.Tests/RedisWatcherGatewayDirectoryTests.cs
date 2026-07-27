using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using ChatApp.Realtime.Infrastructure.Redis.Routing;
using Microsoft.Extensions.Logging.Abstractions;
using StackExchange.Redis;
using Testcontainers.Redis;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// P0-4：验证 <see cref="RedisWatcherGatewayDirectory"/> 的注册/注销幂等性约束。
/// <para>
/// 使用 Testcontainers.Redis 启动真实 Redis 实例，覆盖单实例/多实例/多观察者/批量查询/租约过期等场景。
/// </para>
/// </summary>
public sealed class RedisWatcherGatewayDirectoryTests : IAsyncLifetime
{
    private readonly RedisContainer _redis = new RedisBuilder()
        .WithImage("redis:7-alpine")
        .Build();

    private RoutingMetrics _metrics = null!;
    private RealtimeGarnetClient _client = null!;
    private RedisWatcherGatewayDirectory _directory = null!;

    public async Task InitializeAsync()
    {
        await _redis.StartAsync();

        _metrics = new RoutingMetrics();
        _client = new RealtimeGarnetClient(
            _redis.GetConnectionString(),
            NullLogger<RealtimeGarnetClient>.Instance);
        _directory = new RedisWatcherGatewayDirectory(
            _client,
            _metrics,
            NullLogger<RedisWatcherGatewayDirectory>.Instance);
    }

    public async Task DisposeAsync()
    {
        _metrics.Dispose();
        _client.Dispose();
        await _redis.DisposeAsync().AsTask();
    }

    /// <summary>
    /// 每个测试开始前清空 Redis，避免测试间状态干扰。
    /// </summary>
    private async Task FlushDatabaseAsync()
    {
        var db = _client.GetDatabase();
        await db.ExecuteAsync("FLUSHDB");
    }

    [Fact]
    public async Task Register_IsIdempotent_ForSameTriple()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // 重复注册相同三元组：应仅刷新 score，不产生重复 member。
        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var instances = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);

        var single = Assert.Single(instances);
        Assert.Equal(instanceId, single);
    }

    [Fact]
    public async Task Unregister_IsIdempotent_ForNonExistentRelation()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        // 先注册一个真实存在的观察关系。
        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // 注销一个从未注册的关系：不应抛异常。
        await _directory.UnregisterWatchersAsync(
            watcherUserId: 9999,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // 已存在的注册不应受影响。
        var instances = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);

        var single = Assert.Single(instances);
        Assert.Equal(instanceId, single);
    }

    [Fact]
    public async Task Unregister_Twice_IsIdempotent()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        await _directory.UnregisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // 第二次注销：不存在的 member 应为无操作，不抛异常。
        await _directory.UnregisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var instances = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);

        Assert.Empty(instances);
    }

    [Fact]
    public async Task MultipleWatchers_OnSameInstance_AllTracked()
    {
        await FlushDatabaseAsync();

        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        // 两个不同 watcher 在同一 instance 上观察同一 watchedUser。
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1001,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1002,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var instances = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        // 同一 instance 应去重为 1 个。
        var single = Assert.Single(instances);
        Assert.Equal(instanceId, single);

        // 注销其中一个 watcher：instance 仍应被返回（另一个 watcher 仍活跃）。
        await _directory.UnregisterWatchersAsync(
            watcherUserId: 1001,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var afterFirstUnregister = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        var stillPresent = Assert.Single(afterFirstUnregister);
        Assert.Equal(instanceId, stillPresent);

        // 注销最后一个 watcher：instance 不再被返回。
        await _directory.UnregisterWatchersAsync(
            watcherUserId: 1002,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var afterSecondUnregister = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        Assert.Empty(afterSecondUnregister);
    }

    [Fact]
    public async Task MultipleInstances_ForSameWatchedUser_AllTracked()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserId = 2001;
        const string instanceA = "inst-a";
        const string instanceB = "inst-b";

        // 同一 watcher 在两个不同 instance 上观察同一 watchedUser。
        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceA,
            CancellationToken.None);
        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceB,
            CancellationToken.None);

        var instances = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);

        Assert.Equal(2, instances.Count);
        Assert.Contains(instanceA, instances);
        Assert.Contains(instanceB, instances);
    }

    [Fact]
    public async Task DifferentWatchedUsers_AreIndependent()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserA = 2001;
        const long watchedUserB = 2002;
        const string instanceId = "inst-a";

        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserA },
            instanceId,
            CancellationToken.None);

        var forB = await _directory.GetWatcherGatewaysAsync(
            watchedUserB,
            CancellationToken.None);
        Assert.Empty(forB);

        var forA = await _directory.GetWatcherGatewaysAsync(
            watchedUserA,
            CancellationToken.None);
        var single = Assert.Single(forA);
        Assert.Equal(instanceId, single);
    }

    [Fact]
    public async Task BatchQuery_MultipleWatchedUsers()
    {
        await FlushDatabaseAsync();

        const long watchedUser1 = 2001;
        const long watchedUser2 = 2002;
        const long watchedUser3 = 2003;
        const string instanceA = "inst-a";
        const string instanceB = "inst-b";
        const string instanceC = "inst-c";

        await _directory.RegisterWatchersAsync(
            watcherUserId: 1001,
            new[] { watchedUser1 },
            instanceA,
            CancellationToken.None);
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1002,
            new[] { watchedUser2 },
            instanceB,
            CancellationToken.None);
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1003,
            new[] { watchedUser3 },
            instanceC,
            CancellationToken.None);

        var mapping = await _directory.GetWatcherGatewaysManyAsync(
            new[] { watchedUser1, watchedUser2, watchedUser3 },
            CancellationToken.None);

        Assert.Equal(3, mapping.Count);

        var instancesForUser1 = Assert.Single(mapping[watchedUser1]);
        Assert.Equal(instanceA, instancesForUser1);

        var instancesForUser2 = Assert.Single(mapping[watchedUser2]);
        Assert.Equal(instanceB, instancesForUser2);

        var instancesForUser3 = Assert.Single(mapping[watchedUser3]);
        Assert.Equal(instanceC, instancesForUser3);
    }

    [Fact]
    public async Task LeaseExpiry_CleansUpStaleEntries()
    {
        await FlushDatabaseAsync();

        const long watcherUserId = 1001;
        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        await _directory.RegisterWatchersAsync(
            watcherUserId,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // 注册后应能查到。
        var beforeExpiry = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        var singleBefore = Assert.Single(beforeExpiry);
        Assert.Equal(instanceId, singleBefore);

        // 直接操纵 ZSET：将 member 的 score 设置为已过期的时间戳。
        // 实现内部 key 格式为 watchers:{watchedUserId}:instances（见 RedisWatcherGatewayDirectory 文档）。
        var db = _client.GetDatabase();
        var key = $"watchers:{watchedUserId}:instances";
        var members = await db.SortedSetRangeByRankAsync(key, start: 0, stop: -1);
        Assert.NotEmpty(members);

        var expiredScore = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 1;
        foreach (var member in members)
        {
            await db.SortedSetAddAsync(key, member, expiredScore);
        }

        // 查询时实现会先 ZREMRANGEBYSCORE 清理 score <= now 的过期成员，再返回存活成员。
        // 已过期的 member 应被清理，查询返回空集合。
        var afterExpiry = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        Assert.Empty(afterExpiry);
    }

    [Fact]
    public async Task Reconnect_WithDifferentWatcher_DoesNotInflateCount()
    {
        await FlushDatabaseAsync();

        const long watchedUserId = 2001;
        const string instanceId = "inst-a";

        // watcher1 注册。
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1001,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        // watcher2 在同一 instance、同一 watchedUser 上注册。
        await _directory.RegisterWatchersAsync(
            watcherUserId: 1002,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var afterBothRegistered = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        // 同一 instance 应去重为 1 个（不应因两个 watcher 而膨胀为 2）。
        var singleAfterBoth = Assert.Single(afterBothRegistered);
        Assert.Equal(instanceId, singleAfterBoth);

        // 注销 watcher1：instance 仍应被返回（watcher2 仍活跃）。
        await _directory.UnregisterWatchersAsync(
            watcherUserId: 1001,
            new[] { watchedUserId },
            instanceId,
            CancellationToken.None);

        var afterFirstUnregister = await _directory.GetWatcherGatewaysAsync(
            watchedUserId,
            CancellationToken.None);
        var singleAfterFirst = Assert.Single(afterFirstUnregister);
        Assert.Equal(instanceId, singleAfterFirst);
    }
}

namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 空实现：所有查询返回空集合，所有写操作为无操作。
/// <para>
/// 用于未启用 Presence 分片路由的场景（如 Server 端、测试桩），
/// 使发布方回退到广播模式。
/// </para>
/// </summary>
public sealed class NullWatcherGatewayDirectory : IWatcherGatewayDirectory
{
    public static NullWatcherGatewayDirectory Instance { get; } = new();

    private NullWatcherGatewayDirectory() { }

    public Task<IReadOnlyList<string>> GetWatcherGatewaysAsync(
        long watchedUserId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
    }

    public Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetWatcherGatewaysManyAsync(
        IReadOnlyList<long> watchedUserIds,
        CancellationToken cancellationToken = default)
    {
        var empty = new Dictionary<long, IReadOnlyList<string>>(0);
        return Task.FromResult<IReadOnlyDictionary<long, IReadOnlyList<string>>>(empty);
    }

    public Task RegisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    public Task UnregisterWatchersAsync(
        long watcherUserId,
        IReadOnlyList<long> watchedUserIds,
        string instanceId,
        CancellationToken cancellationToken = default)
    {
        return Task.CompletedTask;
    }

    /// <summary>
    /// P0-9：空实现返回空集合，使发布方在无目录时再次回退到广播。
    /// </summary>
    public Task<IReadOnlyList<string>> ListActiveShardsAsync(
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult<IReadOnlyList<string>>(Array.Empty<string>());
    }
}

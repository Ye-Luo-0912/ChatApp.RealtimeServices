namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 空实现：始终返回空集合。用于未配置路由目录或 Redis 不可用时的回退。
/// <para>
/// 调用方收到空集合后应回退到广播模式，保证不丢事件。
/// </para>
/// </summary>
public sealed class NullGatewayDirectory : IGatewayDirectory
{
    private static readonly IReadOnlyList<string> Empty = Array.Empty<string>();
    private static readonly IReadOnlyDictionary<long, IReadOnlyList<string>> EmptyDict =
        new Dictionary<long, IReadOnlyList<string>>(0);

    public static NullGatewayDirectory Instance { get; } = new();

    public Task<IReadOnlyList<string>> GetOnlineGatewaysAsync(
        long userId,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(Empty);
    }

    public Task<IReadOnlyDictionary<long, IReadOnlyList<string>>> GetOnlineGatewaysManyAsync(
        IReadOnlyList<long> userIds,
        CancellationToken cancellationToken = default)
    {
        return Task.FromResult(EmptyDict);
    }
}

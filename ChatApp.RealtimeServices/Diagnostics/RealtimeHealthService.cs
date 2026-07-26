using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace ChatApp.RealtimeServices.Diagnostics;

public sealed class RealtimeHealthService
{
    private static readonly TimeSpan DependencyTimeout = TimeSpan.FromSeconds(2);
    private readonly IServiceProvider _services;
    private readonly RealtimeReadinessState _readinessState;
    private readonly RealtimeOptions _options;
    private readonly NatsOptions _natsOptions;

    public RealtimeHealthService(
        IServiceProvider services,
        RealtimeReadinessState readinessState,
        IOptions<RealtimeOptions> options,
        IOptions<NatsOptions> natsOptions)
    {
        _services = services;
        _readinessState = readinessState;
        _options = options.Value;
        _natsOptions = natsOptions.Value;
    }

    public async Task<RealtimeHealthSnapshot> CheckAsync(CancellationToken ct = default)
    {
        var workers = _readinessState.GetSnapshot(
            TimeSpan.FromMilliseconds(_options.ReadinessHeartbeatTimeoutMs));
        var dependencies = new Dictionary<string, string>(StringComparer.Ordinal);

        var nats = _services.GetService<NatsConnectionClient>();
        dependencies["nats"] = nats is null
            ? "not_configured"
            : await CheckAsync(token => nats.PingAsync(token), ct).ConfigureAwait(false);

        var database = _services.GetService<RealtimeDatabaseClient>();
        dependencies["postgres"] = database is null || !database.IsConfigured
            ? "not_configured"
            : await CheckAsync(database.PingAsync, ct).ConfigureAwait(false);

        var redis = _services.GetService<RealtimeGarnetClient>();
        dependencies["garnet"] = redis is null
            ? "not_configured"
            : await CheckAsync(token => redis.PingAsync(token), ct).ConfigureAwait(false);

        // not_configured：可选依赖（如 Development 下未配 Garnet）不阻断就绪。
        var dependenciesHealthy = dependencies.Values.All(static status =>
            status is "healthy" or "not_configured");

        // P0-2：暴露当前路由模式与目录实现类型，便于运维确认分片是否真正接通。
        var gatewayDirectory = _services.GetService<IGatewayDirectory>();
        var watcherDirectory = _services.GetService<IWatcherGatewayDirectory>();
        var routing = new RoutingReadinessInfo(
            Mode: _natsOptions.Routing.Mode,
            GatewayDirectoryType: gatewayDirectory?.GetType().Name ?? "None",
            WatcherDirectoryType: watcherDirectory?.GetType().Name ?? "None",
            ShardSubjectPattern: _natsOptions.Routing.RealtimeEventsShardSubjectPattern);

        return new RealtimeHealthSnapshot(
            workers.IsReady && dependenciesHealthy,
            workers,
            dependencies,
            routing,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
    }

    private static async Task<string> CheckAsync(
        Func<CancellationToken, Task> check,
        CancellationToken outerToken)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(outerToken);
        timeout.CancelAfter(DependencyTimeout);
        try
        {
            await check(timeout.Token).ConfigureAwait(false);
            return "healthy";
        }
        catch (OperationCanceledException) when (!outerToken.IsCancellationRequested)
        {
            return "timeout";
        }
        catch (Exception ex)
        {
            return $"unhealthy:{ex.GetType().Name}";
        }
    }
}

public sealed record RealtimeHealthSnapshot(
    bool IsReady,
    RealtimeReadinessSnapshot Workers,
    IReadOnlyDictionary<string, string> Dependencies,
    RoutingReadinessInfo Routing,
    long CheckedAtMs);

public sealed record RoutingReadinessInfo(
    EventRoutingMode Mode,
    string GatewayDirectoryType,
    string WatcherDirectoryType,
    string? ShardSubjectPattern);

using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Infrastructure.Redis.Calls;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using ChatApp.Realtime.Infrastructure.Redis.Routing;
using ChatApp.Realtime.Infrastructure.Redis.State;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Redis.DependencyInjection;

public static class RealtimeRedisRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureRedis(
        this IServiceCollection services,
        string? garnetConnectionString)
    {
        if (string.IsNullOrWhiteSpace(garnetConnectionString))
        {
            return services;
        }

        services.AddSingleton(sp => new RealtimeGarnetClient(
            garnetConnectionString,
            sp.GetRequiredService<ILogger<RealtimeGarnetClient>>()));
        services.RemoveAll<IRealtimeStateStore>();
        services.AddSingleton<IRealtimeStateStore, RedisRealtimeStateStore>();

        // P0-2：注册真实路由目录实现，使分片路由在默认装配路径下生效。
        // 使用 TryAdd 以便 NATS 注册阶段在无 Redis 时仍能兜底注册 Null* 实现。
        services.TryAddSingleton<RoutingMetrics>();
        services.TryAddSingleton<IGatewayDirectory, RedisGatewayDirectory>();
        services.TryAddSingleton<IWatcherGatewayDirectory, RedisWatcherGatewayDirectory>();

        // CALL-CTRL-1：通话临时状态存储覆盖为 Redis/Garnet（TTL + CAS 原子迁移）。
        // 仍只保存控制元数据，绝不保存 SDP/ICE 载荷。
        services.RemoveAll<ICallStateStore>();
        services.AddSingleton<ICallStateStore, RedisCallStateStore>();

        return services;
    }
}

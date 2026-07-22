using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Infrastructure.Redis.Clients;
using ChatApp.Realtime.Infrastructure.Redis.State;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.DependencyInjection.Extensions;

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

        return services;
    }
}

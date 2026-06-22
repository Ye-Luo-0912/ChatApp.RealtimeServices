using ChatApp.Realtime.Infrastructure.Redis.Clients;
using Microsoft.Extensions.DependencyInjection;
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

        return services;
    }
}

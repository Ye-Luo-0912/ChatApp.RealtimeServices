using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Auth;
using ChatApp.Realtime.Infrastructure.Clients;
using ChatApp.Realtime.Infrastructure.Data;
using ChatApp.Realtime.Infrastructure.Events;
using ChatApp.Realtime.Infrastructure.Health;
using ChatApp.Realtime.Infrastructure.Messaging;
using ChatApp.Realtime.Infrastructure.Queueing;
using ChatApp.Realtime.Infrastructure.State;
using ChatApp.Realtime.Infrastructure.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.DependencyInjection;

public static class RealtimeInfrastructureRegistration
{
    /// <summary>
    /// 添加实时基础设施服务到依赖注入容器中。
    /// </summary>
    /// <param name="services">服务集合，用于注册服务。</param>
    /// <param name="garnetConnectionString">Garnet数据库的连接字符串。</param>
    /// <param name="realtimeDatabaseConnectionString">实时数据库的连接字符串，可为空。</param>
    /// <param name="realtimeDatabaseSchema">实时数据库模式名称。</param>
    /// <param name="messageStoreProvider">消息存储提供者标识符。</param>
    /// <param name="queueOptions">队列选项配置。</param>
    /// <returns>添加了实时基础设施服务的服务集合。</returns>
    public static IServiceCollection AddRealtimeInfrastructure(
        this IServiceCollection services,
        string garnetConnectionString,
        string? realtimeDatabaseConnectionString,
        string realtimeDatabaseSchema,
        string messageStoreProvider,
        RealtimeQueueOptions queueOptions)
    {
        services.AddSingleton(queueOptions);

        services.AddSingleton(sp => new RealtimeGarnetClient(
            garnetConnectionString,
            sp.GetRequiredService<ILogger<RealtimeGarnetClient>>()));

        services.AddSingleton(sp => new RealtimeDatabaseClient(
            realtimeDatabaseConnectionString,
            sp.GetRequiredService<ILogger<RealtimeDatabaseClient>>()));

        services.AddSingleton(new RealtimeDatabaseSchema(realtimeDatabaseSchema));

        if (ShouldUseEfCoreMessageStore(realtimeDatabaseConnectionString, messageStoreProvider))
        {
            RealtimeDbContext.ConfigureSchema(realtimeDatabaseSchema);
            services.AddPooledDbContextFactory<RealtimeDbContext>(options =>
            {
                options.UseNpgsql(realtimeDatabaseConnectionString);
            });
        }

        services.AddSingleton<RealtimeReadinessState>();
        services.AddSingleton<IRealtimeAuthReader, NoopRealtimeAuthReader>();
        services.AddSingleton<IIncomingMessageProcessor, DefaultIncomingMessageProcessor>();
        services.AddSingleton<IRealtimeStateStore, InMemoryRealtimeStateStore>();

        if (ShouldUseNatsQueue(queueOptions))
        {
            services.AddSingleton<NatsConnectionClient>();
            services.AddSingleton<IRealtimeEventPublisher, NatsRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventConsumer, NatsRealtimeEventConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, NatsIncomingMessageConsumer>();
        }
        else
        {
            services.AddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        }

        if (ShouldUseEfCoreMessageStore(realtimeDatabaseConnectionString, messageStoreProvider))
        {
            services.AddSingleton<IRealtimeMessageStore, EfCoreRealtimeMessageStore>();
        }
        else if (ShouldUseNpgsqlMessageStore(realtimeDatabaseConnectionString, messageStoreProvider))
        {
            services.AddSingleton<IRealtimeMessageStore, NpgsqlRealtimeMessageStore>();
        }
        else
        {
            services.AddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();
        }

        return services;
    }

    private static bool ShouldUseEfCoreMessageStore(
        string? realtimeDatabaseConnectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(realtimeDatabaseConnectionString)
               && messageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldUseNpgsqlMessageStore(
        string? realtimeDatabaseConnectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(realtimeDatabaseConnectionString)
               && messageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldUseNatsQueue(RealtimeQueueOptions queueOptions)
    {
        return queueOptions.Provider.Equals("Nats", StringComparison.OrdinalIgnoreCase)
               && !string.IsNullOrWhiteSpace(queueOptions.Endpoint);
    }
}

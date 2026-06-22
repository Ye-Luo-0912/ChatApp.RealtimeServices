using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.DependencyInjection;

namespace ChatApp.Realtime.Infrastructure.Nats.DependencyInjection;

public static class RealtimeNatsRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureNats(
        this IServiceCollection services,
        RealtimeQueueOptions queueOptions)
    {
        if (!ShouldUseNatsQueue(queueOptions))
        {
            return services;
        }

        services.AddSingleton(queueOptions);
        services.AddSingleton<NatsConnectionClient>();
        services.AddSingleton<IRealtimeEventPublisher, NatsRealtimeEventPublisher>();
        services.AddSingleton<IRealtimeEventConsumer, NatsRealtimeEventConsumer>();
        services.AddSingleton<IIncomingMessageConsumer, NatsIncomingMessageConsumer>();

        return services;
    }

    private static bool ShouldUseNatsQueue(RealtimeQueueOptions queueOptions)
    {
        return queueOptions.Provider.Equals("Nats", StringComparison.OrdinalIgnoreCase)
               && !string.IsNullOrWhiteSpace(queueOptions.Endpoint);
    }
}

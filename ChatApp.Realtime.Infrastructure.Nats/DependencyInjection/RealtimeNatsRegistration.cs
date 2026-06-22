using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.JetStream;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ChatApp.Realtime.Infrastructure.Nats.DependencyInjection;

public static class RealtimeNatsRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureNats(
        this IServiceCollection services,
        RealtimeQueueOptions queueOptions,
        JetStreamStreamOptions? jetStreamStreams = null)
    {
        if (!ShouldUseNatsQueue(queueOptions))
        {
            return services;
        }

        services.AddSingleton(queueOptions);
        services.AddSingleton<NatsConnectionClient>();

        if (IsJetStream(queueOptions))
        {
            services.AddSingleton(jetStreamStreams ?? new JetStreamStreamOptions());
            services.AddSingleton<JetStreamContextManager>();

            services.RemoveAll<IRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventPublisher, JetStreamRealtimeEventPublisher>();

            services.RemoveAll<IRealtimeEventConsumer>();
            services.AddSingleton<IRealtimeEventConsumer, JetStreamRealtimeEventConsumer>();

            services.RemoveAll<IIncomingMessageConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, JetStreamIncomingMessageConsumer>();
        }
        else
        {
            services.RemoveAll<IRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventPublisher, NatsRealtimeEventPublisher>();

            services.RemoveAll<IRealtimeEventConsumer>();
            services.AddSingleton<IRealtimeEventConsumer, NatsRealtimeEventConsumer>();

            services.RemoveAll<IIncomingMessageConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, NatsIncomingMessageConsumer>();
        }

        return services;
    }

    private static bool ShouldUseNatsQueue(RealtimeQueueOptions queueOptions)
    {
        return (queueOptions.Provider.Equals("Nats", StringComparison.OrdinalIgnoreCase)
                || queueOptions.Provider.Equals("JetStream", StringComparison.OrdinalIgnoreCase))
               && !string.IsNullOrWhiteSpace(queueOptions.Endpoint);
    }

    private static bool IsJetStream(RealtimeQueueOptions queueOptions)
    {
        return queueOptions.Provider.Equals("JetStream", StringComparison.OrdinalIgnoreCase);
    }
}

using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.Realtime.Infrastructure.Nats.JetStream;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Nats.DependencyInjection;

public static class RealtimeNatsRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureNats(
        this IServiceCollection services,
        RealtimeQueueOptions queueOptions,
        JetStreamOptions? jetStreamOptions = null)
    {
        services.AddSingleton(queueOptions);
        if (!ShouldUseNatsQueue(queueOptions))
        {
            return services;
        }
        services.TryAddSingleton(_ => new NatsTransportMetrics(
            RealtimeNatsTelemetry.MeterName));
        services.TryAddSingleton<RoutingMetrics>();
        services.AddSingleton<NatsConnectionClient>();
        services.RemoveAll<IMessageHistoryQueryConsumer>();
        services.AddSingleton<IMessageHistoryQueryConsumer, NatsMessageHistoryQueryConsumer>();
        services.RemoveAll<IConversationListQueryConsumer>();
        services.AddSingleton<IConversationListQueryConsumer, NatsConversationListQueryConsumer>();
        services.RemoveAll<IConversationMarkReadConsumer>();
        services.AddSingleton<IConversationMarkReadConsumer, NatsConversationMarkReadConsumer>();
        services.RemoveAll<IConversationSetPrefsConsumer>();
        services.AddSingleton<IConversationSetPrefsConsumer, NatsConversationSetPrefsConsumer>();
        services.RemoveAll<IGroupConversationConsumer>();
        services.AddSingleton<IGroupConversationConsumer, NatsGroupConversationConsumer>();
        services.RemoveAll<IMessageRecallConsumer>();
        services.AddSingleton<IMessageRecallConsumer, NatsMessageRecallConsumer>();
        services.RemoveAll<IMessageEditConsumer>();
        services.AddSingleton<IMessageEditConsumer, NatsMessageEditConsumer>();
        services.RemoveAll<IMessageReactionConsumer>();
        services.AddSingleton<IMessageReactionConsumer, NatsMessageReactionConsumer>();
        services.RemoveAll<ISyncBootstrapQueryConsumer>();
        services.AddSingleton<ISyncBootstrapQueryConsumer, NatsSyncBootstrapQueryConsumer>();

        if (IsJetStream(queueOptions))
        {
            services.AddSingleton(jetStreamOptions ?? new JetStreamOptions());
            // Null* 仅暴露私有构造函数 + 静态 Instance（单例），不能用 TryAddSingleton<TImpl>
            // 让 DI 容器构造；直接注册实例，避免 ValidateOnBuild 失败。
            services.TryAddSingleton<IGatewayDirectory>(NullGatewayDirectory.Instance);
            services.TryAddSingleton<IWatcherGatewayDirectory>(NullWatcherGatewayDirectory.Instance);
            services.AddSingleton<JetStreamContextManager>(static sp =>
            {
                var shardPattern = sp.GetRequiredService<RealtimeQueueOptions>()
                    .RealtimeEventsShardSubjectPattern;
                return new JetStreamContextManager(
                    sp.GetRequiredService<NatsConnectionClient>(),
                    sp.GetRequiredService<RealtimeQueueOptions>(),
                    sp.GetRequiredService<JetStreamOptions>(),
                    sp.GetRequiredService<ILogger<JetStreamContextManager>>(),
                    shardPattern);
            });

            services.RemoveAll<IRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventPublisher>(static sp =>
            {
                var queueOptions = sp.GetRequiredService<RealtimeQueueOptions>();
                return new JetStreamRealtimeEventPublisher(
                    sp.GetRequiredService<JetStreamContextManager>(),
                    sp.GetService<IGatewayDirectory>(),
                    queueOptions.RealtimeEventsShardSubjectPattern,
                    sp.GetService<RoutingMetrics>());
            });
            services.RemoveAll<IRealtimeEventConsumer>();
            services.AddSingleton<IRealtimeEventConsumer, JetStreamRealtimeEventConsumer>();
            services.RemoveAll<IIncomingMessageConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, JetStreamIncomingMessageConsumer>();
            services.RemoveAll<IMessageReceiptConsumer>();
            services.AddSingleton<IMessageReceiptConsumer, JetStreamMessageReceiptConsumer>();
            services.RemoveAll<IDeadLetterPublisher>();
            services.AddSingleton<IDeadLetterPublisher, JetStreamDeadLetterPublisher>();
        }
        else
        {
            // Null* 仅暴露私有构造函数 + 静态 Instance（单例），不能用 TryAddSingleton<TImpl>
            // 让 DI 容器构造；直接注册实例，避免 ValidateOnBuild 失败。
            services.TryAddSingleton<IGatewayDirectory>(NullGatewayDirectory.Instance);
            services.TryAddSingleton<IWatcherGatewayDirectory>(NullWatcherGatewayDirectory.Instance);
            services.RemoveAll<IRealtimeEventPublisher>();
            services.AddSingleton<IRealtimeEventPublisher>(static sp =>
            {
                var queueOptions = sp.GetRequiredService<RealtimeQueueOptions>();
                return new NatsRealtimeEventPublisher(
                    queueOptions,
                    sp.GetRequiredService<NatsConnectionClient>(),
                    sp.GetRequiredService<ILogger<NatsRealtimeEventPublisher>>(),
                    sp.GetService<IGatewayDirectory>(),
                    queueOptions.RealtimeEventsShardSubjectPattern,
                    sp.GetService<RoutingMetrics>());
            });
            services.RemoveAll<IRealtimeEventConsumer>();
            services.AddSingleton<IRealtimeEventConsumer, NatsRealtimeEventConsumer>();
            services.RemoveAll<IIncomingMessageConsumer>();
            services.AddSingleton<IIncomingMessageConsumer, NatsIncomingMessageConsumer>();
            services.RemoveAll<IMessageReceiptConsumer>();
            services.AddSingleton<IMessageReceiptConsumer, NatsMessageReceiptConsumer>();
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

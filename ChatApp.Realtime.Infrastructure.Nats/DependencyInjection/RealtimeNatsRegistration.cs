using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Diagnostics;
using ChatApp.Realtime.Infrastructure.Nats.JetStream;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using ChatApp.Realtime.Integration;
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
        // Reliability-4：始终注册 JetStreamOptions（即使 Noop 模式），
        // 使 Worker 能注入并读取 AckWait 配置以驱动 In-Progress ACK 守卫。
        // Noop 模式下 AckWait=0，ProgressAckGuard.Start 返回 null（空操作）。
        services.TryAddSingleton(jetStreamOptions ?? new JetStreamOptions());
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

        // Reliability-4：Null* 目录始终注册（查询路径的 IGatewayDirectory 依赖注入需要）。
        // Null* 仅暴露私有构造函数 + 静态 Instance（单例），不能用 TryAddSingleton<TImpl>
        // 让 DI 容器构造；直接注册实例，避免 ValidateOnBuild 失败。
        services.TryAddSingleton<IGatewayDirectory>(NullGatewayDirectory.Instance);
        services.TryAddSingleton<IWatcherGatewayDirectory>(NullWatcherGatewayDirectory.Instance);

        if (IsJetStream(queueOptions))
        {
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
                    sp.GetService<RoutingMetrics>(),
                    sp.GetService<IWatcherGatewayDirectory>(),
                    sp.GetService<RealtimeMetrics>(),
                    sp.GetService<IConversationGatewayDirectory>(),
                    sp.GetService<IRealtimeMessageBus>(),
                    sp.GetService<ILogger<JetStreamRealtimeEventPublisher>>(),
                    queueOptions.ShardPublishParallelism);
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
            // Reliability-4：durable 命令（入站消息、回执、实时事件、死信）禁止使用 Core NATS。
            // Core NATS 无持久化、无重试、无 DLQ 语义，durable 命令在此模式下会静默丢失数据。
            // Mode=Core 时仅注册查询 Consumer（Core NATS request/reply，ephemeral 语义），
            // durable 接口保持 Noop（由 RealtimeCoreRegistration 注册的默认实现）。
            // 如需启用 durable 命令，必须配置 Nats:Mode=JetStream。
            throw new InvalidOperationException(
                "Nats:Mode=Core 不再支持 durable 命令（入站消息/回执/实时事件/死信）。" +
                "durable 命令必须使用 JetStream（Nats:Mode=JetStream）。" +
                "Core NATS 仅用于查询 request/reply（ephemeral 语义）。" +
                "如不需要 durable 命令，请留空 Nats:Url 使队列回退为 Noop。");
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

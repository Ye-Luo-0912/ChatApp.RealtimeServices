using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ChatApp.Realtime.Integration.DependencyInjection;

public static class RealtimeIntegrationRegistration
{
    public static IServiceCollection AddChatAppRealtimeIntegration(
        this IServiceCollection services,
        RealtimeIntegrationOptions options)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(options.Url);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.InstanceId);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.MessageHistoryQueriesSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConversationListQueriesSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConversationMarkReadsSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.ConversationSetPrefsSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.MessageRecallsSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.MessageEditsSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.SyncBootstrapQueriesSubject);
        ArgumentException.ThrowIfNullOrWhiteSpace(options.GroupConversationsSubject);
        if (options.HistoryRequestTimeoutMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.HistoryRequestTimeoutMs));
        if (options.Replicas <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.Replicas));

        services.AddSingleton(options);
        services.AddSingleton(_ => new NatsTransportMetrics(
            RealtimeIntegrationTelemetry.ActivitySourceName));
        services.TryAddSingleton<RoutingMetrics>();
        services.TryAddSingleton<IGatewayDirectory, NullGatewayDirectory>();
        services.TryAddSingleton<IWatcherGatewayDirectory, NullWatcherGatewayDirectory>();
        services.AddSingleton<IRealtimeMessageBus, NatsRealtimeMessageBus>();
        return services;
    }
}

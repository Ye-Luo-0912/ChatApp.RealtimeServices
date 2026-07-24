using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Integration.Configuration;
using Microsoft.Extensions.DependencyInjection;

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
        if (options.HistoryRequestTimeoutMs <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.HistoryRequestTimeoutMs));
        if (options.Replicas <= 0)
            throw new ArgumentOutOfRangeException(nameof(options.Replicas));

        services.AddSingleton(options);
        services.AddSingleton(_ => new NatsTransportMetrics(
            RealtimeIntegrationTelemetry.ActivitySourceName));
        services.AddSingleton<IRealtimeMessageBus, NatsRealtimeMessageBus>();
        return services;
    }
}

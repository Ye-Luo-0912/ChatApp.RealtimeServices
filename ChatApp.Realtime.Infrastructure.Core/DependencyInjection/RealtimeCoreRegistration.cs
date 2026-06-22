using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Auth;
using ChatApp.Realtime.Infrastructure.Core.Events;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.State;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ChatApp.Realtime.Infrastructure.Core.DependencyInjection;

public static class RealtimeCoreRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureCore(this IServiceCollection services)
    {
        services.TryAddSingleton<RealtimeReadinessState>();
        services.TryAddSingleton<IRealtimeAuthReader, NoopRealtimeAuthReader>();
        services.TryAddSingleton<IRealtimeStateStore, InMemoryRealtimeStateStore>();
        services.TryAddSingleton<IIncomingMessageProcessor, DefaultIncomingMessageProcessor>();

        services.TryAddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
        services.TryAddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
        services.TryAddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        services.TryAddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();

        return services;
    }
}

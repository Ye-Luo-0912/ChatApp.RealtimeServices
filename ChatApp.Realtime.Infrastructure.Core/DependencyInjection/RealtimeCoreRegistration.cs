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

namespace ChatApp.Realtime.Infrastructure.Core.DependencyInjection;

public static class RealtimeCoreRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureCore(this IServiceCollection services)
    {
        services.AddSingleton<RealtimeReadinessState>();
        services.AddSingleton<IRealtimeAuthReader, NoopRealtimeAuthReader>();
        services.AddSingleton<IRealtimeStateStore, InMemoryRealtimeStateStore>();
        services.AddSingleton<IIncomingMessageProcessor, DefaultIncomingMessageProcessor>();

        services.AddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
        services.AddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
        services.AddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        services.AddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();

        return services;
    }
}

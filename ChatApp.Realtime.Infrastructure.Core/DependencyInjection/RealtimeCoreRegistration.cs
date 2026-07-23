using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Auth;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Events;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.State;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Core.Sync;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace ChatApp.Realtime.Infrastructure.Core.DependencyInjection;

public static class RealtimeCoreRegistration
{
    public static IServiceCollection AddRealtimeInfrastructureCore(this IServiceCollection services)
    {
        services.TryAddSingleton<RealtimeReadinessState>();
        services.TryAddSingleton<RealtimeMetrics>();
        services.TryAddSingleton<IRealtimeOutboxSignal, RealtimeOutboxSignal>();
        services.TryAddSingleton<IRealtimeAuthReader, NoopRealtimeAuthReader>();
        services.TryAddSingleton<IRealtimeStateStore, InMemoryRealtimeStateStore>();
        services.TryAddSingleton<IIncomingMessageProcessor, DefaultIncomingMessageProcessor>();
        services.TryAddSingleton<IUserAccountDeletedProcessor, DefaultUserAccountDeletedProcessor>();
        services.TryAddSingleton<IMessageReceiptProcessor, DefaultMessageReceiptProcessor>();
        services.TryAddSingleton<IMessageHistoryQueryProcessor, DefaultMessageHistoryQueryProcessor>();
        services.TryAddSingleton<IConversationListQueryProcessor, DefaultConversationListQueryProcessor>();
        services.TryAddSingleton<IConversationMarkReadProcessor, DefaultConversationMarkReadProcessor>();
        services.TryAddSingleton<IConversationSetPrefsProcessor, DefaultConversationSetPrefsProcessor>();
        services.TryAddSingleton<IMessageRecallProcessor, DefaultMessageRecallProcessor>();
        services.TryAddSingleton<ISyncBootstrapQueryProcessor, DefaultSyncBootstrapQueryProcessor>();

        services.TryAddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
        services.TryAddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
        services.TryAddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        services.TryAddSingleton<IMessageReceiptConsumer, NoopMessageReceiptConsumer>();
        services.TryAddSingleton<IMessageHistoryQueryConsumer, NoopMessageHistoryQueryConsumer>();
        services.TryAddSingleton<IConversationListQueryConsumer, NoopConversationListQueryConsumer>();
        services.TryAddSingleton<IConversationMarkReadConsumer, NoopConversationMarkReadConsumer>();
        services.TryAddSingleton<IConversationSetPrefsConsumer, NoopConversationSetPrefsConsumer>();
        services.TryAddSingleton<IMessageRecallConsumer, NoopMessageRecallConsumer>();
        services.TryAddSingleton<ISyncBootstrapQueryConsumer, NoopSyncBootstrapQueryConsumer>();
        services.TryAddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();
        services.TryAddSingleton<IRealtimeAttachmentStore, NoopRealtimeAttachmentStore>();
        services.TryAddSingleton<IRealtimeMessageHistoryStore, NoopRealtimeMessageHistoryStore>();
        services.TryAddSingleton<IRealtimeConversationStore, NoopRealtimeConversationStore>();
        services.TryAddSingleton<IRealtimeDeviceSyncCursorStore, NoopRealtimeDeviceSyncCursorStore>();
        services.TryAddSingleton<IRealtimeOutboxStore, NoopRealtimeOutboxStore>();
        services.TryAddSingleton<IDeadLetterPublisher, NoopDeadLetterPublisher>();

        return services;
    }
}

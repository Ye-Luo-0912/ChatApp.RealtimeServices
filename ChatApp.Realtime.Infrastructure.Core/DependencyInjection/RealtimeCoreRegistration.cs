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
using Microsoft.Extensions.Configuration;
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
        services.TryAddSingleton<IGroupConversationProcessor, DefaultGroupConversationProcessor>();
        services.TryAddSingleton<IMessageRecallProcessor, DefaultMessageRecallProcessor>();
        services.TryAddSingleton<IMessageEditProcessor, DefaultMessageEditProcessor>();
        services.TryAddSingleton<IMessageReactionProcessor, DefaultMessageReactionProcessor>();
        services.TryAddSingleton(new MessageEditOptions());
        services.TryAddSingleton(new MessageRecallOptions());
        services.TryAddSingleton(new MessageReactionOptions());
        services.TryAddSingleton(BindSyncBootstrapOptions);
        services.TryAddSingleton<ISyncBootstrapQueryProcessor, DefaultSyncBootstrapQueryProcessor>();

        services.TryAddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
        services.TryAddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
        services.TryAddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        services.TryAddSingleton<IMessageReceiptConsumer, NoopMessageReceiptConsumer>();
        services.TryAddSingleton<IMessageHistoryQueryConsumer, NoopMessageHistoryQueryConsumer>();
        services.TryAddSingleton<IConversationListQueryConsumer, NoopConversationListQueryConsumer>();
        services.TryAddSingleton<IConversationMarkReadConsumer, NoopConversationMarkReadConsumer>();
        services.TryAddSingleton<IConversationSetPrefsConsumer, NoopConversationSetPrefsConsumer>();
        services.TryAddSingleton<IGroupConversationConsumer, NoopGroupConversationConsumer>();
        services.TryAddSingleton<IMessageRecallConsumer, NoopMessageRecallConsumer>();
        services.TryAddSingleton<IMessageEditConsumer, NoopMessageEditConsumer>();
        services.TryAddSingleton<IMessageReactionConsumer, NoopMessageReactionConsumer>();
        services.TryAddSingleton<ISyncBootstrapQueryConsumer, NoopSyncBootstrapQueryConsumer>();
        services.TryAddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();
        services.TryAddSingleton<IRealtimeReadReceiptStore>(NoopRealtimeReadReceiptStore.Instance);
        services.TryAddSingleton<IRealtimeAttachmentStore, NoopRealtimeAttachmentStore>();
        services.TryAddSingleton<IRealtimeReactionStore, NoopRealtimeReactionStore>();
        services.TryAddSingleton<IRealtimeMessageHistoryStore, NoopRealtimeMessageHistoryStore>();
        services.TryAddSingleton<IRealtimeConversationStore, NoopRealtimeConversationStore>();
        services.TryAddSingleton<IRealtimeGroupStore, NoopRealtimeGroupStore>();
        services.TryAddSingleton<IRealtimeDeviceSyncCursorStore, NoopRealtimeDeviceSyncCursorStore>();
        services.TryAddSingleton<IRealtimeOutboxStore, NoopRealtimeOutboxStore>();
        services.TryAddSingleton<IRealtimeMessageRetentionStore, NoopRealtimeMessageRetentionStore>();
        services.TryAddSingleton<IUserDeletionTombstoneStore, NoopUserDeletionTombstoneStore>();
        services.TryAddSingleton<IUserExistenceChecker>(NoopUserExistenceChecker.Instance);
        services.TryAddSingleton<ICommandIdempotencyLedger, NoopCommandIdempotencyLedger>();
        services.TryAddSingleton<IGroupOperationAuditStore, NoopGroupOperationAuditStore>();
        services.TryAddSingleton<IMembershipPeriodStore, NoopMembershipPeriodStore>();
        services.TryAddSingleton<IDeadLetterPublisher, NoopDeadLetterPublisher>();
        services.TryAddSingleton<IBlockListStore>(NoopBlockListStore.Instance);
        services.TryAddSingleton<IDirectMessagePolicy>(NoopDirectMessagePolicy.Instance);
        services.TryAddSingleton<IPrivacySettingStore>(NoopPrivacySettingStore.Instance);
        services.TryAddSingleton<IMessageRateLimiter>(NoopMessageRateLimiter.Instance);

        return services;
    }

    /// <summary>
    /// Fallback factory used when the host (e.g. RealtimeServicesRegistration) has not already
    /// registered a bound/validated <see cref="SyncBootstrapOptions"/> singleton. Binds directly
    /// from <c>SyncBootstrap</c> when an <see cref="IConfiguration"/> is available; otherwise
    /// falls back to defaults (all knobs disabled).
    /// </summary>
    private static SyncBootstrapOptions BindSyncBootstrapOptions(IServiceProvider provider)
    {
        var configuration = provider.GetService<IConfiguration>();
        if (configuration is null)
            return new SyncBootstrapOptions();

        return configuration.GetSection(SyncBootstrapOptions.SectionName).Get<SyncBootstrapOptions>()
            ?? new SyncBootstrapOptions();
    }
}

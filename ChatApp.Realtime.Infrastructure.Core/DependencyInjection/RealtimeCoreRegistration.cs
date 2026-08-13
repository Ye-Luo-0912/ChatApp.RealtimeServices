using ChatApp.Realtime.Abstractions.Attachments;
using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.State;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Attachments;
using ChatApp.Realtime.Infrastructure.Core.Auth;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Events;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Core.Messaging;
using ChatApp.Realtime.Infrastructure.Core.Messaging.History;
using ChatApp.Realtime.Infrastructure.Core.Relationships;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Calls;
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
        // 同一有界实例同时承载唤醒与提交后 event-id 提示，避免为每条消息创建任务、
        // 定时器或独立队列。自定义 IRealtimeOutboxSignal 注册仍可覆盖并自动退化为扫描路径。
        services.TryAddSingleton<RealtimeOutboxSignal>();
        services.TryAddSingleton<IRealtimeOutboxSignal>(provider =>
            provider.GetRequiredService<RealtimeOutboxSignal>());
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
        services.TryAddSingleton<IAttachmentFinalizeProcessor, DefaultAttachmentFinalizeProcessor>();
        services.TryAddSingleton<IMessageRecallProcessor, DefaultMessageRecallProcessor>();
        services.TryAddSingleton<IMessageEditProcessor, DefaultMessageEditProcessor>();
        services.TryAddSingleton<IMessageReactionProcessor, DefaultMessageReactionProcessor>();
        services.TryAddSingleton(new MessageEditOptions());
        services.TryAddSingleton(new MessageRecallOptions());
        services.TryAddSingleton(new MessageReactionOptions());
        services.TryAddSingleton(BindSyncBootstrapOptions);
        // Do not inject the legacy relationship store into the default bootstrap path.
        // Explicit relationship sync requests fail closed inside the processor until
        // ChatApp.Server publishes an authoritative projection.
        services.TryAddSingleton<ISyncBootstrapQueryProcessor>(provider =>
            new DefaultSyncBootstrapQueryProcessor(
                provider.GetRequiredService<IRealtimeConversationStore>(),
                provider.GetRequiredService<IRealtimeMessageHistoryStore>(),
                provider.GetRequiredService<IRealtimeDeviceSyncCursorStore>(),
                provider.GetRequiredService<IRealtimeAttachmentStore>(),
                provider.GetRequiredService<IRealtimeReactionStore>(),
                provider.GetRequiredService<SyncBootstrapOptions>(),
                provider.GetService<IRelationshipProjectionStore>(),
                provider.GetService<IRelationshipSyncCursorStore>()));

        services.TryAddSingleton<IRealtimeEventPublisher, NoopRealtimeEventPublisher>();
        services.TryAddSingleton<IRealtimeEventConsumer, NoopRealtimeEventConsumer>();
        services.TryAddSingleton<IIncomingMessageConsumer, NoopIncomingMessageConsumer>();
        services.TryAddSingleton<IMessageReceiptConsumer, NoopMessageReceiptConsumer>();
        services.TryAddSingleton<IMessageHistoryQueryConsumer, NoopMessageHistoryQueryConsumer>();
        services.TryAddSingleton<IConversationListQueryConsumer, NoopConversationListQueryConsumer>();
        services.TryAddSingleton<IConversationMarkReadConsumer, NoopConversationMarkReadConsumer>();
        services.TryAddSingleton<IConversationSetPrefsConsumer, NoopConversationSetPrefsConsumer>();
        services.TryAddSingleton<IGroupConversationConsumer, NoopGroupConversationConsumer>();
        services.TryAddSingleton<IAttachmentFinalizeConsumer, NoopAttachmentFinalizeConsumer>();
        services.TryAddSingleton<IMessageRecallConsumer, NoopMessageRecallConsumer>();
        services.TryAddSingleton<IMessageEditConsumer, NoopMessageEditConsumer>();
        services.TryAddSingleton<IMessageReactionConsumer, NoopMessageReactionConsumer>();
        services.TryAddSingleton<ISyncBootstrapQueryConsumer, NoopSyncBootstrapQueryConsumer>();
        services.TryAddSingleton<IRelationshipCommandConsumer, NoopRelationshipCommandConsumer>();
        services.TryAddSingleton<IRelationshipListQueryConsumer, NoopRelationshipListQueryConsumer>();
        services.TryAddSingleton<IRealtimeMessageStore, NoopRealtimeMessageStore>();
        services.TryAddSingleton<IRealtimeReadReceiptStore>(NoopRealtimeReadReceiptStore.Instance);
        services.TryAddSingleton<IRealtimeAttachmentStore, NoopRealtimeAttachmentStore>();
        services.TryAddSingleton<IRealtimeReactionStore, NoopRealtimeReactionStore>();
        services.TryAddSingleton<IRealtimeMessageHistoryStore, NoopRealtimeMessageHistoryStore>();
        services.TryAddSingleton<IRealtimeConversationStore, NoopRealtimeConversationStore>();
        services.TryAddSingleton<IRealtimeGroupStore, NoopRealtimeGroupStore>();
        services.TryAddSingleton<IRealtimeDeviceSyncCursorStore, NoopRealtimeDeviceSyncCursorStore>();
        services.TryAddSingleton<IRealtimeOutboxStore, NoopRealtimeOutboxStore>();
        services.TryAddSingleton<IRelationshipProjectionStore, UnavailableRelationshipProjectionStore>();
        services.TryAddSingleton<IRelationshipProjectionQueryStore>(
            UnavailableRelationshipProjectionQueryStore.Instance);
        services.TryAddSingleton<IRelationshipProjectionOpsQueryStore>(
            UnavailableRelationshipProjectionOpsQueryStore.Instance);
        services.TryAddSingleton<IRealtimeMessageRetentionStore, NoopRealtimeMessageRetentionStore>();
        services.TryAddSingleton<IAccountCleanupJobStore, NoopAccountCleanupJobStore>();
        services.TryAddSingleton<IUserDeletionTombstoneStore, NoopUserDeletionTombstoneStore>();
        services.TryAddSingleton<IUserExistenceChecker>(NoopUserExistenceChecker.Instance);
        services.TryAddSingleton<ICommandIdempotencyLedger, NoopCommandIdempotencyLedger>();
        services.TryAddSingleton<IGroupOperationAuditStore, NoopGroupOperationAuditStore>();
        services.TryAddSingleton<IMembershipPeriodStore, NoopMembershipPeriodStore>();
        services.TryAddSingleton<IDeadLetterPublisher, NoopDeadLetterPublisher>();
        services.TryAddSingleton<IBlockListStore>(NoopBlockListStore.Instance);
        services.TryAddSingleton<IRelationshipStore, NoopRelationshipStore>();
        services.TryAddSingleton<IRelationshipSyncCursorStore, NoopRelationshipSyncCursorStore>();
        // ChatApp.Server owns relationship mutations. Keep the NATS worker alive so
        // legacy TCP callers receive an immediate, explicit failure instead of a
        // timeout, but never let the default runtime write the legacy realtime tables.
        services.TryAddSingleton<IRelationshipCommandProcessor>(
            ServerAuthoritativeRelationshipCommandProcessor.Instance);
        services.TryAddSingleton<IRelationshipListQueryProcessor>(
            ServerAuthoritativeRelationshipListQueryProcessor.Instance);
        services.TryAddSingleton<IDirectMessagePolicy>(NoopDirectMessagePolicy.Instance);
        services.TryAddSingleton<IPrivacySettingStore>(NoopPrivacySettingStore.Instance);
        services.TryAddSingleton<IMessageRateLimiter>(NoopMessageRateLimiter.Instance);

        // ---- CALL-CTRL-1：临时通话信令状态机 ----
        // 通话 SDP/ICE 只经临时信令路径转发，绝不进入 PostgreSQL / 持久化 Outbox / JetStream。
        // 默认内存临时状态存储 + 空转发器 + 默认 grant 校验；生产由 Redis/NATS 覆盖。
        services.TryAddSingleton<CallPolicyOptions>(new CallPolicyOptions());
        services.TryAddSingleton<CallMetrics>();
        services.TryAddSingleton<ICallStateStore, InMemoryCallStateStore>();
        services.TryAddSingleton<ICallAuditStore, InMemoryCallAuditStore>();
        services.TryAddSingleton<ICallGrantVerifier, DefaultCallGrantVerifier>();
        services.TryAddSingleton<ICallSignalForwarder>(NoopCallSignalForwarder.Instance);
        services.TryAddSingleton<TimeProvider>(TimeProvider.System);
        services.TryAddSingleton<ICallControlProcessor, DefaultCallControlProcessor>();
        services.TryAddSingleton<ICallControlConsumer>(NoopCallControlConsumer.Instance);

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

using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Initialization;
using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Postgres.DependencyInjection;

public static class RealtimePostgresRegistration
{
    public static IServiceCollection AddRealtimeInfrastructurePostgres(
        this IServiceCollection services,
        string? connectionString,
        string schema,
        string messageStoreProvider)
    {
        services.AddSingleton(sp => new RealtimeDatabaseClient(
            connectionString,
            sp.GetRequiredService<ILogger<RealtimeDatabaseClient>>()));

        services.AddSingleton(new RealtimeDatabaseSchema(schema));
        services.RemoveAll<IRealtimeOpsQueryStore>();
        services.AddSingleton<IRealtimeOpsQueryStore, NpgsqlRealtimeOpsQueryStore>();

        // P0-8：消息变更（撤回 / 编辑 / Reaction）统一权限策略，防止离群用户修改旧群消息。
        services.RemoveAll<IConversationMessageMutationPolicy>();
        services.AddSingleton<IConversationMessageMutationPolicy, PostgresConversationMessageMutationPolicy>();

        if (ShouldUseEfCoreMessageStore(connectionString, messageStoreProvider))
        {
            RealtimeDbContext.ConfigureSchema(schema);
            services.AddPooledDbContextFactory<RealtimeDbContext>(options =>
            {
                options.UseNpgsql(connectionString);
            });

            services.RemoveAll<IRealtimeMessageStore>();
            services.AddSingleton<IRealtimeMessageStore, EfCoreRealtimeMessageStore>();
        }
        else if (ShouldUseNpgsqlMessageStore(connectionString, messageStoreProvider))
        {
            services.RemoveAll<IRealtimeMessageStore>();
            // Perf-3：注入 ICommandIdempotencyLedger，将幂等账本查询与记录下沉到 SaveAsync 事务内。
            services.AddSingleton<IRealtimeMessageStore>(sp => new NpgsqlRealtimeMessageStore(
                sp.GetRequiredService<RealtimeDatabaseClient>(),
                sp.GetRequiredService<RealtimeDatabaseSchema>(),
                sp.GetRequiredService<IConversationMessageMutationPolicy>(),
                sp.GetRequiredService<ILogger<NpgsqlRealtimeMessageStore>>(),
                sp.GetService<RealtimeMetrics>(),
                sp.GetRequiredService<ICommandIdempotencyLedger>()));
        }

        if (!string.IsNullOrWhiteSpace(connectionString)
            && (messageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase)
                || messageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase)))
        {
            services.RemoveAll<IRealtimeMessageHistoryStore>();
            services.AddSingleton<IRealtimeMessageHistoryStore, NpgsqlRealtimeMessageHistoryStore>();
            services.RemoveAll<IRealtimeConversationStore>();
            // Reliability-4：传入 RealtimeMetrics，由 session 在事务提交成功后记录 outbox 入队行数。
            services.AddSingleton<IRealtimeConversationStore>(sp => new NpgsqlRealtimeConversationStore(
                sp.GetRequiredService<RealtimeDatabaseClient>(),
                sp.GetRequiredService<RealtimeDatabaseSchema>(),
                sp.GetService<RealtimeMetrics>()));
            services.RemoveAll<IRealtimeGroupStore>();
            // 审计 Outbox：注入 IGroupOperationAuditStore，使群操作审计在业务事务内写入。
            // Membership periods：注入 IMembershipPeriodStore，使群操作在事务内记录入群/离群 period。
            services.AddSingleton<IRealtimeGroupStore>(sp => new NpgsqlRealtimeGroupStore(
                sp.GetRequiredService<RealtimeDatabaseClient>(),
                sp.GetRequiredService<RealtimeDatabaseSchema>(),
                sp.GetRequiredService<IGroupOperationAuditStore>(),
                sp.GetRequiredService<IMembershipPeriodStore>(),
                sp.GetRequiredService<IUserExistenceChecker>()));
            services.RemoveAll<IUserExistenceChecker>();
            services.AddSingleton<IUserExistenceChecker, NpgsqlUserExistenceChecker>();
            services.RemoveAll<IBlockListStore>();
            services.AddSingleton<IBlockListStore, NpgsqlBlockListStore>();
            services.RemoveAll<IRelationshipStore>();
            services.AddSingleton<IRelationshipStore>(sp => new NpgsqlRelationshipStore(
                sp.GetRequiredService<RealtimeDatabaseClient>(),
                sp.GetRequiredService<RealtimeDatabaseSchema>()));
            services.RemoveAll<IDirectMessagePolicy>();
            services.AddSingleton<IDirectMessagePolicy, NpgsqlDirectMessagePolicy>();
            services.RemoveAll<IPrivacySettingStore>();
            services.AddSingleton<IPrivacySettingStore, NpgsqlPrivacySettingStore>();
            services.RemoveAll<IMessageRateLimiter>();
            services.AddSingleton<IMessageRateLimiter, NpgsqlMessageRateLimiter>();
            services.RemoveAll<IRealtimeDeviceSyncCursorStore>();
            services.AddSingleton<IRealtimeDeviceSyncCursorStore, NpgsqlRealtimeDeviceSyncCursorStore>();
            services.RemoveAll<IRealtimeOutboxStore>();
            services.AddSingleton<IRealtimeOutboxStore, NpgsqlRealtimeOutboxStore>();
            services.RemoveAll<IRealtimeAttachmentStore>();
            services.AddSingleton<IRealtimeAttachmentStore, NpgsqlRealtimeAttachmentStore>();
            services.RemoveAll<IRealtimeReactionStore>();
            // Reliability-4：传入 RealtimeMetrics，由 session 在事务提交成功后记录 outbox 入队行数。
            services.AddSingleton<IRealtimeReactionStore>(sp => new NpgsqlRealtimeReactionStore(
                sp.GetRequiredService<RealtimeDatabaseClient>(),
                sp.GetRequiredService<RealtimeDatabaseSchema>(),
                sp.GetRequiredService<IConversationMessageMutationPolicy>(),
                sp.GetService<RealtimeMetrics>()));
            services.RemoveAll<IRealtimeMessageRetentionStore>();
            services.AddSingleton<IRealtimeMessageRetentionStore, NpgsqlRealtimeMessageRetentionStore>();
            services.RemoveAll<IRealtimeReadReceiptStore>();
            services.AddSingleton<IRealtimeReadReceiptStore, NpgsqlRealtimeReadReceiptStore>();
            // LongTerm-1：用户删除 tombstone + 独立命令幂等账本。
            services.RemoveAll<IUserDeletionTombstoneStore>();
            services.AddSingleton<IUserDeletionTombstoneStore, NpgsqlUserDeletionTombstoneStore>();
            services.RemoveAll<ICommandIdempotencyLedger>();
            services.AddSingleton<ICommandIdempotencyLedger, NpgsqlCommandIdempotencyLedger>();
            // Feature 2：群操作审计。
            services.RemoveAll<IGroupOperationAuditStore>();
            services.AddSingleton<IGroupOperationAuditStore, NpgsqlGroupOperationAuditStore>();
            // LongTerm-2：账号清理可续跑 Saga 作业存储。
            services.RemoveAll<IAccountCleanupJobStore>();
            services.AddSingleton<IAccountCleanupJobStore, NpgsqlAccountCleanupJobStore>();
        }

        // Membership periods：入群/离群时间段记录，用于历史可见性过滤。
        if (!string.IsNullOrWhiteSpace(connectionString)
            && (messageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase)
                || messageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase)))
        {
            services.RemoveAll<IMembershipPeriodStore>();
            services.AddSingleton<IMembershipPeriodStore, NpgsqlMembershipPeriodStore>();
        }

        return services;
    }

    public static IServiceCollection AddRealtimeDatabaseInitializer(
        this IServiceCollection services,
        bool initializeSchemaOnStart,
        string? connectionString)
    {
        if (!initializeSchemaOnStart || string.IsNullOrWhiteSpace(connectionString))
        {
            return services;
        }

        services.AddHostedService<RealtimeDatabaseInitializer>();
        return services;
    }

    private static bool ShouldUseEfCoreMessageStore(
        string? connectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(connectionString)
               && messageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase);
    }

    private static bool ShouldUseNpgsqlMessageStore(
        string? connectionString,
        string messageStoreProvider)
    {
        return !string.IsNullOrWhiteSpace(connectionString)
               && messageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase);
    }
}


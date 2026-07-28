using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.Realtime.Infrastructure.Postgres.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Redis.DependencyInjection;
using ChatApp.Realtime.Infrastructure.Redis.Routing;
using ChatApp.RealtimeServices.Concurrency;
using ChatApp.RealtimeServices.Diagnostics;
using ChatApp.RealtimeServices.Options;
using ChatApp.RealtimeServices.Workers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace ChatApp.RealtimeServices.DependencyInjection;

public static class RealtimeServicesRegistration
{
    public static IServiceCollection AddRealtimeServices(
        this IServiceCollection services,
        IConfiguration configuration,
        IHostEnvironment environment)
    {
        var realtimeOptions = BindRealtimeOptions(configuration);
        var natsOptions = BindNatsOptions(configuration);
        var databaseOptions = BindDatabaseOptions(configuration);
        var connectionOptions = BindConnectionOptions(configuration);
        var outboxOptions = BindOutboxOptions(configuration);
        ValidateProductionConfiguration(environment, natsOptions, databaseOptions, connectionOptions);
        GateEfCoreMessageStore(environment, databaseOptions);
        var warnings = BuildWarnings(configuration, natsOptions, databaseOptions, connectionOptions);
        var trustSettings = RealtimeNatsTrustSettings.From(
            natsOptions.Trust,
            environment.IsDevelopment());

        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(realtimeOptions));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(natsOptions));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(databaseOptions));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(connectionOptions));
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(outboxOptions));
        services.AddSingleton(BindMessageEditOptions(configuration));
        services.AddSingleton(BindMessageRecallOptions(configuration));
        services.AddSingleton(BindMessageReactionOptions(configuration));
        services.AddSingleton(BindSyncBootstrapOptions(configuration));
        var messageRetentionOptions = BindMessageRetentionOptions(configuration);
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(messageRetentionOptions));
        services.AddSingleton(messageRetentionOptions);
        var idempotencyOptions = BindIdempotencyOptions(configuration);
        services.AddSingleton(Microsoft.Extensions.Options.Options.Create(idempotencyOptions));
        services.AddSingleton(idempotencyOptions);
        services.AddSingleton(trustSettings);
        services.AddSingleton(new RealtimeConfigurationWarnings(warnings));
        services.AddSingleton<RealtimeHealthService>();
        services.AddSingleton<RealtimeQueryConcurrencyGate>();
        // Perf-1：群消息按 ConversationId 分区的策略。默认实现足够，注册为单例。
        services.TryAddSingleton<IMessagePartitionKeySelector, DefaultMessagePartitionKeySelector>();

        // Perf-8：Outbox Dead 行归档接收器。默认使用 NullDeadLetterArchiveSink（直接物理删除）。
        // 业务方可注册自定义 IDeadLetterArchiveSink 覆盖，并在 OutboxOptions.DeadArchiveSink 指定名称。
        services.TryAddSingleton<IDeadLetterArchiveSink, NullDeadLetterArchiveSink>();

        services.AddRealtimeInfrastructureCore();
        services.AddRealtimeInfrastructureRedis(connectionOptions.Garnet);
        services.AddRealtimeInfrastructurePostgres(
            connectionOptions.RealtimeDatabase,
            databaseOptions.Schema,
            databaseOptions.MessageStoreProvider);
        services.AddRealtimeInfrastructureNats(
            CreateRealtimeQueueOptions(natsOptions),
            natsOptions.JetStream);
        services.AddRealtimeDatabaseInitializer(
            databaseOptions.InitializeSchemaOnStart,
            connectionOptions.RealtimeDatabase);

        // Perf-2：会话级受众路由目录。有 Garnet/Redis 配置时使用 Redis 实现，
        // 未配置时使用空实现（Publisher 收到 LookupFailure 后回退到 per-user 路由，保证不丢事件）。
        // 与 IGatewayDirectory 装配方式一致：Redis 在前注册真实实现，无 Redis 时回退 Null。
        if (!string.IsNullOrWhiteSpace(connectionOptions.Garnet))
        {
            services.TryAddSingleton<IConversationGatewayDirectory, RedisConversationGatewayDirectory>();
        }
        else
        {
            services.TryAddSingleton<IConversationGatewayDirectory>(NullConversationGatewayDirectory.Instance);
        }

        services.AddHostedService<RealtimeStartupReporter>();

        services.AddHostedService<IncomingMessageWorker>();
        services.AddHostedService<AccountCleanupWorker>();
        services.AddHostedService<RealtimeEventWorker>();
        services.AddHostedService<MessageReceiptWorker>();
        services.AddHostedService<MessageHistoryQueryWorker>();
        services.AddHostedService<ConversationListQueryWorker>();
        services.AddHostedService<ConversationMarkReadWorker>();
        services.AddHostedService<ConversationSetPrefsWorker>();
        services.AddHostedService<GroupConversationWorker>();
        services.AddHostedService<MessageRecallWorker>();
        services.AddHostedService<MessageEditWorker>();
        services.AddHostedService<MessageReactionWorker>();
        services.AddHostedService<ConversationSyncBootstrapWorker>();
        services.AddHostedService<OutboxPublisherWorker>();
        services.AddHostedService<OutboxCleanupWorker>();
        services.AddHostedService<MessageRetentionWorker>();
        // LongTerm-1：独立幂等账本 + 用户删除 tombstone 的周期 GC（不阻断就绪）。
        services.AddHostedService<IdempotencyGCWorker>();

        return services;
    }

    private static MessageEditOptions BindMessageEditOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(MessageEditOptions.SectionName).Get<MessageEditOptions>()
            ?? new MessageEditOptions();
        if (options.MaxAgeMinutes < 0)
            throw new InvalidOperationException("MessageEdit:MaxAgeMinutes 不能为负数。");
        return options;
    }

    private static MessageRecallOptions BindMessageRecallOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(MessageRecallOptions.SectionName).Get<MessageRecallOptions>()
            ?? new MessageRecallOptions();
        if (options.MaxAgeMinutes < 0)
            throw new InvalidOperationException("MessageRecall:MaxAgeMinutes 不能为负数。");
        return options;
    }

    private static MessageReactionOptions BindMessageReactionOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(MessageReactionOptions.SectionName).Get<MessageReactionOptions>()
            ?? new MessageReactionOptions();
        if (options.MaxDistinctEmojisPerMessage < 0)
            throw new InvalidOperationException("MessageReaction:MaxDistinctEmojisPerMessage 不能为负数。");
        if (options.MaxReactionsPerUserPerMessage < 0)
            throw new InvalidOperationException("MessageReaction:MaxReactionsPerUserPerMessage 不能为负数。");
        if (options.MaxEmojiLength <= 0 || options.MaxEmojiLength > 32)
            throw new InvalidOperationException("MessageReaction:MaxEmojiLength 必须在 1–32。");
        return options;
    }

    private static RealtimeOptions BindRealtimeOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("Realtime");
        var options = section.Get<RealtimeOptions>()
            ?? throw new InvalidOperationException("Realtime 配置节缺失。");

        if (string.IsNullOrWhiteSpace(options.ServiceName))
            throw new InvalidOperationException("Realtime:ServiceName 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.InstanceId))
            throw new InvalidOperationException("Realtime:InstanceId 为必填配置。");
        if (options.ProcessingConcurrency <= 0 || options.ProcessingQueueCapacity < options.ProcessingConcurrency)
            throw new InvalidOperationException("Realtime 消费并发度必须大于 0，队列容量不能小于并发度。");
        if (options.HistoryQueryConcurrency <= 0
            || options.HistoryQueryQueueCapacity < options.HistoryQueryConcurrency)
            throw new InvalidOperationException("历史查询并发度必须大于 0，队列容量不能小于并发度。");
        if (options.HistoryQueryWorkerSlots <= 0)
            throw new InvalidOperationException("Realtime:HistoryQueryWorkerSlots 必须大于 0。");

        return options.InstanceId.Equals("auto", StringComparison.OrdinalIgnoreCase)
            ? new RealtimeOptions
            {
                ServiceName = options.ServiceName,
                InstanceId = Environment.MachineName,
                WorkerIntervalMs = options.WorkerIntervalMs,
                EnableDetailedErrors = options.EnableDetailedErrors,
                ProcessingConcurrency = options.ProcessingConcurrency,
                ProcessingQueueCapacity = options.ProcessingQueueCapacity,
                // P0-6：补齐此前重建时遗漏的字段——字节预算与过载配置，
                // 避免 InstanceId=auto 时这些配置被静默重置为默认值。
                ProcessingQueueByteBudget = options.ProcessingQueueByteBudget,
                MaxSinglePayloadBytes = options.MaxSinglePayloadBytes,
                DeadLetterPayloadLimitBytes = options.DeadLetterPayloadLimitBytes,
                HistoryQueryConcurrency = options.HistoryQueryConcurrency,
                HistoryQueryQueueCapacity = options.HistoryQueryQueueCapacity,
                HistoryQueryWorkerSlots = options.HistoryQueryWorkerSlots,
                TransientRetryDelayMs = options.TransientRetryDelayMs,
                PoisonDeliveryThreshold = options.PoisonDeliveryThreshold,
                ReadinessHeartbeatTimeoutMs = options.ReadinessHeartbeatTimeoutMs,
                OverloadEnqueueTimeoutMs = options.OverloadEnqueueTimeoutMs,
                OverloadGateTimeoutMs = options.OverloadGateTimeoutMs,
                OverloadRetryAfterMs = options.OverloadRetryAfterMs
            }
            : options;
    }

    private static NatsOptions BindNatsOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("Nats");
        var options = section.Get<NatsOptions>()
            ?? throw new InvalidOperationException("Nats 配置节缺失。");

        if (string.IsNullOrWhiteSpace(options.QueueGroup))
            throw new InvalidOperationException("Nats:QueueGroup 为必填配置。");
        if (options.Subjects is null)
            throw new InvalidOperationException("Nats:Subjects 配置节缺失。");
        if (string.IsNullOrWhiteSpace(options.Subjects.IncomingMessages))
            throw new InvalidOperationException("Nats:Subjects:IncomingMessages 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.MessageReceipts))
            throw new InvalidOperationException("Nats:Subjects:MessageReceipts 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.RealtimeEvents))
            throw new InvalidOperationException("Nats:Subjects:RealtimeEvents 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.MessageHistoryQueries))
            throw new InvalidOperationException("Nats:Subjects:MessageHistoryQueries 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.ConversationListQueries))
            throw new InvalidOperationException("Nats:Subjects:ConversationListQueries 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.ConversationMarkReads))
            throw new InvalidOperationException("Nats:Subjects:ConversationMarkReads 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.ConversationSetPrefs))
            throw new InvalidOperationException("Nats:Subjects:ConversationSetPrefs 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.MessageRecalls))
            throw new InvalidOperationException("Nats:Subjects:MessageRecalls 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.MessageEdits))
            throw new InvalidOperationException("Nats:Subjects:MessageEdits 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.MessageReactions))
            throw new InvalidOperationException("Nats:Subjects:MessageReactions 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.SyncBootstrapQueries))
            throw new InvalidOperationException("Nats:Subjects:SyncBootstrapQueries 为必填配置。");
        if (string.IsNullOrWhiteSpace(options.Subjects.GroupConversations))
            throw new InvalidOperationException("Nats:Subjects:GroupConversations 为必填配置。");
        if (!options.Mode.Equals("Core", StringComparison.OrdinalIgnoreCase)
            && !options.Mode.Equals("JetStream", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("Nats:Mode 只允许 Core 或 JetStream。");
        if (string.IsNullOrWhiteSpace(options.Subjects.DeadLetters))
            throw new InvalidOperationException("Nats:Subjects:DeadLetters 为必填配置。");

        return new NatsOptions
        {
            Url = options.Url,
            Mode = options.Mode,
            QueueGroup = options.QueueGroup,
            Subjects = options.Subjects,
            JetStream = options.JetStream,
            Auth = options.Auth ?? new NatsAuthOptions(),
            Trust = options.Trust ?? new NatsTrustOptions(),
            Routing = options.Routing ?? new NatsRoutingOptions()
        };
    }

    private static OutboxOptions BindOutboxOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection("Outbox").Get<OutboxOptions>() ?? new OutboxOptions();
        if (options.BatchSize <= 0 || options.PublishConcurrency <= 0 || options.PollIntervalMs <= 0
            || options.LeaseSeconds <= 0 || options.MaxRetryDelaySeconds <= 0
            || options.MaxAttempts <= 0 || options.CleanupBatchSize <= 0 || options.CleanupIntervalMs <= 0)
            throw new InvalidOperationException("Outbox 配置值必须大于 0。");
        if (options.PublishedRetentionHours < 0)
            throw new InvalidOperationException("Outbox:PublishedRetentionHours 不能为负数。");
        if (options.PublishedMaxBatchesPerCycle < 0)
            throw new InvalidOperationException("Outbox:PublishedMaxBatchesPerCycle 不能为负数。");
        if (options.PublishedBatchSleepMs < 0)
            throw new InvalidOperationException("Outbox:PublishedBatchSleepMs 不能为负数。");
        if (options.DeadRetentionDays < 0)
            throw new InvalidOperationException("Outbox:DeadRetentionDays 不能为负数。");
        if (options.DeadMaxRows < 0)
            throw new InvalidOperationException("Outbox:DeadMaxRows 不能为负数。");
        return options;
    }

    private static SyncBootstrapOptions BindSyncBootstrapOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(SyncBootstrapOptions.SectionName).Get<SyncBootstrapOptions>()
            ?? new SyncBootstrapOptions();
        if (options.MaxCatchUpGapMs < 0)
            throw new InvalidOperationException("SyncBootstrap:MaxCatchUpGapMs 不能为负数。");
        if (options.RetentionHorizonMs < 0)
            throw new InvalidOperationException("SyncBootstrap:RetentionHorizonMs 不能为负数。");
        return options;
    }

    private static MessageRetentionOptions BindMessageRetentionOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(MessageRetentionOptions.SectionName).Get<MessageRetentionOptions>()
            ?? new MessageRetentionOptions();
        if (options.RetentionHorizonMs < 0)
            throw new InvalidOperationException("MessageRetention:RetentionHorizonMs 不能为负数。");
        if (options.RetentionDays < 0)
            throw new InvalidOperationException("MessageRetention:RetentionDays 不能为负数。");
        if (options.BatchSize <= 0)
            throw new InvalidOperationException("MessageRetention:BatchSize 必须大于 0。");
        if (options.IntervalMs <= 0)
            throw new InvalidOperationException("MessageRetention:IntervalMs 必须大于 0。");
        if (options.BatchSleepMs < 0)
            throw new InvalidOperationException("MessageRetention:BatchSleepMs 不能为负数。");
        if (options.MaxBatchesPerCycle < 0)
            throw new InvalidOperationException("MessageRetention:MaxBatchesPerCycle 不能为负数。");
        return options;
    }

    private static IdempotencyOptions BindIdempotencyOptions(IConfiguration configuration)
    {
        var options = configuration.GetSection(IdempotencyOptions.SectionName).Get<IdempotencyOptions>()
            ?? new IdempotencyOptions();
        if (options.RetentionHorizonMs < 0)
            throw new InvalidOperationException("Idempotency:RetentionHorizonMs 不能为负数。");
        if (options.RetentionDays < 0)
            throw new InvalidOperationException("Idempotency:RetentionDays 不能为负数。");
        if (options.BatchSize <= 0)
            throw new InvalidOperationException("Idempotency:BatchSize 必须大于 0。");
        if (options.IntervalMs <= 0)
            throw new InvalidOperationException("Idempotency:IntervalMs 必须大于 0。");
        if (options.BatchSleepMs < 0)
            throw new InvalidOperationException("Idempotency:BatchSleepMs 不能为负数。");
        if (options.MaxBatchesPerCycle < 0)
            throw new InvalidOperationException("Idempotency:MaxBatchesPerCycle 不能为负数。");
        return options;
    }

    private static RealtimeDatabaseOptions BindDatabaseOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("RealtimeDatabase");
        var raw = section.Get<RealtimeDatabaseOptions>();

        var provider = Normalize(raw?.MessageStoreProvider) ?? "Noop";
        if (!provider.Equals("Noop", StringComparison.OrdinalIgnoreCase)
            && !provider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase)
            && !provider.Equals("EfCore", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("RealtimeDatabase:MessageStoreProvider 只允许 Noop、Npgsql 或 EfCore。");

        return new RealtimeDatabaseOptions
        {
            Schema = Normalize(raw?.Schema) ?? "realtime",
            MessageStoreProvider = provider,
            InitializeSchemaOnStart = raw?.InitializeSchemaOnStart ?? false
        };
    }

    private static RealtimeConnectionOptions BindConnectionOptions(IConfiguration configuration)
    {
        var section = configuration.GetSection("ConnectionStrings");
        var raw = section.Get<RealtimeConnectionOptions>() ?? new RealtimeConnectionOptions();

        return new RealtimeConnectionOptions
        {
            Garnet = Normalize(raw.Garnet),
            RealtimeDatabase = Normalize(configuration["ConnectionStrings:RealtimeDatabase"])
                ?? Normalize(configuration["ConnectionStrings:DefaultConnection"])
        };
    }

    private static RealtimeQueueOptions CreateRealtimeQueueOptions(NatsOptions options)
    {
        // P0-2：将路由分片配置映射到 RealtimeQueueOptions，使 Server 端默认装配路径接通分片。
        // Sharded 模式下使用配置的 pattern；留空则保持 null（广播，向后兼容）。
        string? shardPattern = null;
        if (options.Routing.Mode == EventRoutingMode.Sharded)
        {
            shardPattern = string.IsNullOrWhiteSpace(options.Routing.RealtimeEventsShardSubjectPattern)
                ? "chat.realtime-events.{0}"
                : options.Routing.RealtimeEventsShardSubjectPattern;
        }

        return new RealtimeQueueOptions
        {
            Provider = string.IsNullOrWhiteSpace(options.Url)
                ? "Noop"
                : options.Mode.Equals("JetStream", StringComparison.OrdinalIgnoreCase)
                    ? "JetStream"
                    : "Nats",
            Endpoint = options.Url,
            ConsumerGroup = options.QueueGroup,
            Topics = new RealtimeQueueTopics
            {
                IncomingMessages = options.Subjects.IncomingMessages,
                MessageReceipts = options.Subjects.MessageReceipts,
                RealtimeEvents = options.Subjects.RealtimeEvents,
                AccountCleanup = options.Subjects.AccountCleanup,
                MessageHistoryQueries = options.Subjects.MessageHistoryQueries,
                ConversationListQueries = options.Subjects.ConversationListQueries,
                ConversationMarkReads = options.Subjects.ConversationMarkReads,
                ConversationSetPrefs = options.Subjects.ConversationSetPrefs,
                MessageRecalls = options.Subjects.MessageRecalls,
                MessageEdits = options.Subjects.MessageEdits,
                MessageReactions = options.Subjects.MessageReactions,
                SyncBootstrapQueries = options.Subjects.SyncBootstrapQueries,
                GroupConversations = options.Subjects.GroupConversations,
                MessagePersistence = options.Subjects.MessagePersistence,
                DeadLetters = options.Subjects.DeadLetters
            },
            RealtimeEventsShardSubjectPattern = shardPattern,
            ShardPublishParallelism = options.Routing.ShardPublishParallelism
        };
    }

    private static void GateEfCoreMessageStore(
        IHostEnvironment environment,
        RealtimeDatabaseOptions databaseOptions)
    {
        if (!databaseOptions.MessageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase))
            return;

        // EfCore 消息存储不绑定附件，生产/预发禁止；仅 Development / Testing 可用。
        if (environment.IsDevelopment() || environment.IsEnvironment("Testing"))
            return;

        throw new InvalidOperationException(
            "RealtimeDatabase:MessageStoreProvider=EfCore 仅允许 Development/Testing。" +
            "生产必须使用 Npgsql（附件绑定、回执与删除语义完整）。");
    }

    private static void ValidateProductionConfiguration(
        IHostEnvironment environment,
        NatsOptions natsOptions,
        RealtimeDatabaseOptions databaseOptions,
        RealtimeConnectionOptions connectionOptions)
    {
        if (environment.IsDevelopment())
            return;

        if (string.IsNullOrWhiteSpace(natsOptions.Url)
            || !natsOptions.Mode.Equals("JetStream", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException("非开发环境必须配置 NATS JetStream，禁止回退到 Core/Noop。");
        if (natsOptions.JetStream is null || natsOptions.JetStream.Replicas < 3)
            throw new InvalidOperationException("非开发环境 JetStream 副本数必须至少为 3。");
        if (string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase)
            || !databaseOptions.MessageStoreProvider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase))
            throw new InvalidOperationException(
                "非开发环境必须配置真实 PostgreSQL 消息存储，且 RealtimeDatabase:MessageStoreProvider 必须为 Npgsql。" +
                "EfCore 路径不支持附件绑定/回执/删除一致性，仅允许 Development/Testing。");
        if (string.IsNullOrWhiteSpace(connectionOptions.Garnet))
            throw new InvalidOperationException("非开发环境必须配置 Garnet/Redis 共享状态存储。");

        if (!HasNatsAuth(natsOptions.Auth))
        {
            throw new InvalidOperationException(
                "非开发环境必须配置 Nats:Auth（CredsFile / NKeyFile / Username / Token / Seed / NKey）。" +
                "网关身份头不能替代 NATS 账户认证；请同时配置 subject ACL。");
        }

        // 二次校验：身份头只做一致性检查，不能关闭。
        var requireIdentity = natsOptions.Trust.RequireGatewayIdentity ?? true;
        if (!requireIdentity)
        {
            throw new InvalidOperationException(
                "非开发环境必须保持 Nats:Trust:RequireGatewayIdentity=true（账户认证之外的二次校验）。");
        }

        if (databaseOptions.InitializeSchemaOnStart)
        {
            throw new InvalidOperationException(
                "非开发环境禁止 RealtimeDatabase:InitializeSchemaOnStart=true。" +
                "009/010 等重迁移应由独立 Job/CLI 执行（CREATE INDEX CONCURRENTLY + 检查点续跑）。");
        }

        // P0-2：分片模式要求真实路由目录（Redis/Garnet），否则会回退到广播，规模上去后无收益。
        if (natsOptions.Routing.Mode == EventRoutingMode.Sharded
            && string.IsNullOrWhiteSpace(connectionOptions.Garnet))
        {
            throw new InvalidOperationException(
                "非开发环境启用 Nats:Routing:Mode=Sharded 时必须配置 ConnectionStrings:Garnet，" +
                "以注册真实的 IGatewayDirectory / IWatcherGatewayDirectory。" +
                "未配置 Garnet 时仅能注册 Null* 目录，分片路由会回退到广播，违背分片目的。");
        }
    }

    private static IReadOnlyList<string> BuildWarnings(
        IConfiguration configuration,
        NatsOptions natsOptions,
        RealtimeDatabaseOptions databaseOptions,
        RealtimeConnectionOptions connectionOptions)
    {
        var warnings = new List<string>();

        if (string.IsNullOrWhiteSpace(connectionOptions.Garnet))
        {
            warnings.Add("ConnectionStrings:Garnet 未配置，Garnet/Redis 客户端不会注册。");
        }

        if (string.IsNullOrWhiteSpace(natsOptions.Url))
        {
            warnings.Add("Nats:Url 未配置，实时队列会回退为空实现。");
        }

        if (string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("ConnectionStrings:RealtimeDatabase 未配置，实时数据库客户端不会建立连接。");
        }
        else if (string.IsNullOrWhiteSpace(configuration["ConnectionStrings:RealtimeDatabase"])
                 && !string.IsNullOrWhiteSpace(configuration["ConnectionStrings:DefaultConnection"]))
        {
            warnings.Add("ConnectionStrings:RealtimeDatabase 未配置，已回退使用 ConnectionStrings:DefaultConnection。");
        }

        if (string.IsNullOrWhiteSpace(configuration["RealtimeDatabase:Schema"]))
        {
            warnings.Add($"RealtimeDatabase:Schema 未配置，已使用默认值 '{databaseOptions.Schema}'。");
        }

        if (databaseOptions.MessageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase)
            && string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("实时消息存储已指定为 EF Core，但 ConnectionStrings:RealtimeDatabase 未配置，运行时会回退到 P0 默认空实现。");
        }

        if (databaseOptions.MessageStoreProvider.Equals("EfCore", StringComparison.OrdinalIgnoreCase))
        {
            warnings.Add("当前启用了 EF Core 消息存储。Native AOT 发布下需要 compiled model，否则运行时可能失败。容器环境建议使用 Npgsql。");
        }

        if (databaseOptions.InitializeSchemaOnStart
            && string.IsNullOrWhiteSpace(connectionOptions.RealtimeDatabase))
        {
            warnings.Add("已要求启动时初始化实时数据库表结构，但实时数据库连接字符串未配置，初始化不会执行。");
        }

        if (!HasNatsAuth(natsOptions.Auth))
        {
            warnings.Add(
                "Nats:Auth 未配置。生产环境必须启用 NATS 账户认证与 subject ACL；参见 docs/nats-trust-boundary.md。");
        }

        return warnings;
    }

    private static bool HasNatsAuth(NatsAuthOptions? auth) =>
        auth is not null
        && (!string.IsNullOrWhiteSpace(auth.CredsFile)
            || !string.IsNullOrWhiteSpace(auth.NKeyFile)
            || !string.IsNullOrWhiteSpace(auth.Username)
            || !string.IsNullOrWhiteSpace(auth.Token)
            || !string.IsNullOrWhiteSpace(auth.Seed)
            || !string.IsNullOrWhiteSpace(auth.NKey));

    private static string? Normalize(string? value)
    {
        return string.IsNullOrWhiteSpace(value) ? null : value.Trim();
    }
}

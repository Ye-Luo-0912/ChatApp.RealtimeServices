using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Health;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.JetStream;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.RealtimeServices.Options;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.DependencyInjection;

namespace ChatApp.RealtimeServices.Diagnostics;

public sealed class RealtimeStartupReporter : IHostedService
{
    /// <summary>
    /// Reliability-2：必需 Worker 列表。这些 Worker 必须全部启动并处于 Running 状态，
    /// <see cref="RealtimeReadinessState.GetSnapshot"/> 才会返回 IsReady=true。
    /// 清理类 Worker（AccountCleanup / OutboxCleanup / MessageRetention）不阻断就绪。
    /// </summary>
    internal static readonly string[] RequiredWorkerNames =
    [
        "IncomingMessageWorker",
        "MessageReceiptWorker",
        "OutboxPublisherWorker",
        "RealtimeEventWorker",
        "MessageHistoryQueryWorker",
        "ConversationListQueryWorker",
        "ConversationMarkReadWorker",
        "ConversationSetPrefsWorker",
        "GroupConversationWorker",
        "MessageRecallWorker",
        "MessageEditWorker",
        "MessageReactionWorker",
        "ConversationSyncBootstrapWorker"
    ];

    /// <summary>
    /// Reliability-3：非关键（清理类）Worker 列表。这些 Worker 的健康状态会上报但不会阻断就绪判定。
    /// </summary>
    internal static readonly string[] NonCriticalWorkerNames =
    [
        "AccountCleanupWorker",
        "OutboxCleanupWorker",
        "MessageRetentionWorker",
        "OutboxMetricsCollector",
        "IdempotencyGCWorker"
    ];

    private readonly IHostEnvironment _environment;
    private readonly IOptions<RealtimeOptions> _realtimeOptions;
    private readonly IOptions<NatsOptions> _natsOptions;
    private readonly IOptions<RealtimeDatabaseOptions> _databaseOptions;
    private readonly IOptions<RealtimeConnectionOptions> _connectionOptions;
    private readonly IOptions<IdempotencyOptions> _idempotencyOptions;
    private readonly IOptions<OutboxOptions> _outboxOptions;
    private readonly RealtimeConfigurationWarnings _warnings;
    private readonly RealtimeReadinessState _readinessState;
    private readonly IServiceProvider _services;
    private readonly ILogger<RealtimeStartupReporter> _logger;

    public RealtimeStartupReporter(
        IHostEnvironment environment,
        IOptions<RealtimeOptions> realtimeOptions,
        IOptions<NatsOptions> natsOptions,
        IOptions<RealtimeDatabaseOptions> databaseOptions,
        IOptions<RealtimeConnectionOptions> connectionOptions,
        IOptions<IdempotencyOptions> idempotencyOptions,
        IOptions<OutboxOptions> outboxOptions,
        RealtimeConfigurationWarnings warnings,
        RealtimeReadinessState readinessState,
        IServiceProvider services,
        ILogger<RealtimeStartupReporter> logger)
    {
        _environment = environment;
        _realtimeOptions = realtimeOptions;
        _natsOptions = natsOptions;
        _databaseOptions = databaseOptions;
        _connectionOptions = connectionOptions;
        _idempotencyOptions = idempotencyOptions;
        _outboxOptions = outboxOptions;
        _warnings = warnings;
        _readinessState = readinessState;
        _services = services;
        _logger = logger;
    }

    /// <summary>
    /// 异步启动实时服务，并记录相关配置信息。
    /// </summary>
    /// <param name="cancellationToken">用于取消操作的令牌。</param>
    /// <returns>返回一个表示异步操作的任务。</returns>
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        // Reliability-2：注册必需 Worker，使 GetSnapshot 能验证它们是否全部启动。
        foreach (var name in RequiredWorkerNames)
            _readinessState.RegisterRequiredWorker(name);

        // Reliability-3：注册非关键 Worker，状态上报但不阻断就绪。
        foreach (var name in NonCriticalWorkerNames)
            _readinessState.RegisterNonCriticalWorker(name);

        var realtime = _realtimeOptions.Value;
        var nats = _natsOptions.Value;

        _logger.LogInformation(
            "实时服务配置已加载。服务名={ServiceName}；实例={InstanceId}；环境={Environment}；工作循环间隔毫秒={WorkerIntervalMs}",
            realtime.ServiceName,
            realtime.InstanceId,
            _environment.EnvironmentName,
            realtime.WorkerIntervalMs);

        _logger.LogInformation(
            "实时队列边界已配置。队列类型=NATS；地址={Url}；队列组={QueueGroup}；入站消息Subject={IncomingSubject}；回执Subject={ReceiptSubject}；实时事件Subject={EventSubject}；历史查询Subject={HistorySubject}；消息持久化Subject={MessagePersistenceSubject}",
            nats.Url ?? "<未配置>",
            nats.QueueGroup,
            nats.Subjects.IncomingMessages,
            nats.Subjects.MessageReceipts,
            nats.Subjects.RealtimeEvents,
            nats.Subjects.MessageHistoryQueries,
            nats.Subjects.MessagePersistence ?? "<未配置>");

        _logger.LogInformation(
            "实时存储边界已配置。Garnet已配置={GarnetConfigured}；实时数据库已配置={RealtimeDatabaseConfigured}；数据库架构={Schema}；消息存储实现={MessageStoreProvider}；启动时初始化表结构={InitializeSchemaOnStart}",
            !string.IsNullOrWhiteSpace(_connectionOptions.Value.Garnet),
            !string.IsNullOrWhiteSpace(_connectionOptions.Value.RealtimeDatabase),
            _databaseOptions.Value.Schema,
            GetMessageStoreProviderDisplayName(_databaseOptions.Value.MessageStoreProvider),
            _databaseOptions.Value.InitializeSchemaOnStart);

        foreach (var warning in _warnings.Warnings)
        {
            _logger.LogWarning("{Warning}", warning);
        }

        var jetStream = _services.GetService<JetStreamContextManager>();
        if (jetStream is not null)
        {
            await jetStream.EnsureStreamsAsync(cancellationToken).ConfigureAwait(false);
            _logger.LogInformation("JetStream 入站、回执、事件与死信流已完成校准。");
        }

        // P0-2：运行时校验分片路由目录装配，并记录路由模式与目录实现类型。
        var gatewayDirectory = _services.GetService<IGatewayDirectory>();
        var watcherDirectory = _services.GetService<IWatcherGatewayDirectory>();
        var gatewayDirType = gatewayDirectory?.GetType().Name ?? "<未注册>";
        var watcherDirType = watcherDirectory?.GetType().Name ?? "<未注册>";
        var routingMode = nats.Routing.Mode;
        _logger.LogInformation(
            "实时事件路由已装配。模式={RoutingMode}；网关目录={GatewayDirectory}；watcher 目录={WatcherDirectory}",
            routingMode,
            gatewayDirType,
            watcherDirType);

        // 生产环境：分片模式 + Null 目录视为配置错误（回退到广播违背分片目的）。
        if (routingMode == EventRoutingMode.Sharded
            && !_environment.IsDevelopment()
            && (gatewayDirectory is null or NullGatewayDirectory
                || watcherDirectory is null or NullWatcherGatewayDirectory))
        {
            throw new InvalidOperationException(
                $"Nats:Routing:Mode=Sharded 但路由目录为 Null（gateway={gatewayDirType}, watcher={watcherDirType}）。" +
                "请配置 ConnectionStrings:Garnet 以注册 RedisGatewayDirectory / RedisWatcherGatewayDirectory。");
        }

        // LongTerm-1：校验幂等账本保留期 >= JetStream MaxAge。
        // JetStream durable 重建后会以 DeliverPolicy.All 回放旧命令；若账本保留期短于 MaxAge，
        // 则旧命令在账本清理后会被当作新消息重新写入（"复活"）。
        var jetStreamMaxAgeHours = nats.JetStream?.MaxAgeHours ?? 168;
        var jetStreamMaxAgeMs = jetStreamMaxAgeHours * 3_600_000L;
        var idempotencyHorizonMs = _idempotencyOptions.Value
            .ResolveEffectiveHorizonMs(jetStreamMaxAgeMs);
        if (idempotencyHorizonMs < jetStreamMaxAgeMs)
        {
            throw new InvalidOperationException(
                $"Idempotency 保留期（{idempotencyHorizonMs} ms）小于 JetStream MaxAge" +
                $"（{jetStreamMaxAgeMs} ms = {jetStreamMaxAgeHours} h）。" +
                "请增大 Idempotency:RetentionDays / RetentionHorizonMs，" +
                "确保账本保留期不少于 JetStream 最大回放周期，防止旧命令在账本清理后\"复活\"。");
        }

        _logger.LogInformation(
            "幂等账本保留配置。保留窗口毫秒={HorizonMs}；JetStream MaxAge 毫秒={JetStreamMaxAgeMs}；启用={Enabled}",
            idempotencyHorizonMs,
            jetStreamMaxAgeMs,
            _idempotencyOptions.Value.Enabled);

        // Reliability-4：校验 JetStream DuplicateWindow >= Outbox 最坏重试周期。
        // Outbox "发布成功但 MarkPublished 失败"后重试时，依赖 JetStream MsgId 去重防止重复投递。
        // 若去重窗口短于重试周期，窗口过期后的重试会产生重复消息。
        var outboxOpts = _outboxOptions.Value;
        var maxAttempts = outboxOpts.MaxAttempts;
        var maxRetryDelaySec = outboxOpts.MaxRetryDelaySeconds;
        long worstCaseRetrySec = 0;
        for (var i = 1; i < maxAttempts; i++)
        {
            // CalculateRetryDelay: min(MaxRetryDelay, 2^attempt) + jitter(0-500ms), round up to 1s
            worstCaseRetrySec += Math.Min(maxRetryDelaySec, (long)Math.Pow(2, Math.Min(i, 10))) + 1;
        }
        var duplicateWindowSec = (long)(nats.JetStream?.DuplicateWindowMinutes ?? 15) * 60;
        if (duplicateWindowSec < worstCaseRetrySec)
        {
            throw new InvalidOperationException(
                $"JetStream DuplicateWindowMinutes（{nats.JetStream?.DuplicateWindowMinutes ?? 15}）" +
                $"小于 Outbox 最坏重试周期（{worstCaseRetrySec} 秒 = {worstCaseRetrySec / 60.0:F1} 分钟）。" +
                $"配置：MaxAttempts={maxAttempts}, MaxRetryDelaySeconds={maxRetryDelaySec}。" +
                "请增大 Nats:JetStream:DuplicateWindowMinutes，确保去重窗口覆盖整个重试周期，" +
                "防止\"发布成功但 MarkPublished 失败\"场景下重试产生重复投递。");
        }

        _logger.LogInformation(
            "Outbox 重试去重窗口配置。DuplicateWindow 秒={DuplicateWindowSec}；最坏重试周期秒={WorstCaseRetrySec}",
            duplicateWindowSec,
            worstCaseRetrySec);

        var snapshot = _readinessState.GetSnapshot();
        _logger.LogInformation(
            "实时服务就绪状态已初始化。是否就绪={Ready}；工作器数量={WorkerCount}",
            snapshot.IsReady,
            snapshot.Workers.Count);

    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        var snapshot = _readinessState.GetSnapshot();
        _logger.LogInformation(
            "实时服务正在停止。是否就绪={Ready}；工作器数量={WorkerCount}",
            snapshot.IsReady,
            snapshot.Workers.Count);

        return Task.CompletedTask;
    }

    private static string GetMessageStoreProviderDisplayName(string provider)
    {
        if (provider.Equals("EfCore", StringComparison.OrdinalIgnoreCase))
        {
            return "EF Core 数据库存储";
        }

        if (provider.Equals("Npgsql", StringComparison.OrdinalIgnoreCase))
        {
            return "Npgsql 直连数据库存储";
        }

        return provider.Equals("Noop", StringComparison.OrdinalIgnoreCase) ? "P0 空实现" : provider;
    }
}

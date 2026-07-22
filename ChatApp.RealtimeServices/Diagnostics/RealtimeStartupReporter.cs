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
    private readonly IHostEnvironment _environment;
    private readonly IOptions<RealtimeOptions> _realtimeOptions;
    private readonly IOptions<NatsOptions> _natsOptions;
    private readonly IOptions<RealtimeDatabaseOptions> _databaseOptions;
    private readonly IOptions<RealtimeConnectionOptions> _connectionOptions;
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

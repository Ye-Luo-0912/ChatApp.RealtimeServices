using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;

namespace ChatApp.Realtime.Integration;

/// <summary>
/// 管理 <see cref="NatsConnection"/> 与 <see cref="INatsJSContext"/> 的延迟创建，
/// 订阅连接事件并记录连接指标。
/// </summary>
internal sealed class NatsConnectionProvider : IAsyncDisposable
{
    private readonly RealtimeIntegrationOptions _options;
    private readonly NatsTransportMetrics _metrics;
    private readonly ILogger _logger;
    private readonly Lazy<NatsConnection> _client;
    private readonly Lazy<INatsJSContext> _context;

    public NatsConnectionProvider(
        RealtimeIntegrationOptions options,
        NatsTransportMetrics metrics,
        ILogger logger)
    {
        _options = options;
        _metrics = metrics;
        _logger = logger;
        _client = new Lazy<NatsConnection>(CreateClient);
        _context = new Lazy<INatsJSContext>(() => new NatsJSContext(_client.Value));
    }

    /// <summary>
    /// 当前实例的 NATS 客户端（延迟创建）。
    /// </summary>
    public NatsConnection Client => _client.Value;

    /// <summary>
    /// 当前实例的 JetStream 上下文（延迟创建）。
    /// </summary>
    public INatsJSContext Context => _context.Value;

    /// <summary>
    /// 客户端是否已创建（用于 DisposeAsync 判断是否需要清理）。
    /// </summary>
    public bool IsClientCreated => _client.IsValueCreated;

    /// <summary>
    /// 探测 NATS 连接往返延迟。
    /// </summary>
    public async Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
        await _client.Value.PingAsync(ct).ConfigureAwait(false);

    private NatsConnection CreateClient()
    {
        var requestTimeout = TimeSpan.FromMilliseconds(
            Math.Max(_options.HistoryRequestTimeoutMs, 1_000));
        _logger.LogInformation(
            "创建实时集成 NATS 客户端。Client={Client}; Url={Url}",
            _options.ClientName,
            NatsEndpointRedactor.ForLog(_options.Url));
        var connection = new NatsConnection(new NatsOpts
        {
            Url = _options.Url,
            Name = $"{_options.ClientName}-{_options.InstanceId}",
            AuthOpts = CreateAuthOpts(_options.Auth),
            ConnectTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout = requestTimeout,
            CommandTimeout = TimeSpan.FromSeconds(5),
            PingInterval = TimeSpan.FromSeconds(20),
            MaxPingOut = 2,
            MaxReconnectRetry = -1,
            ReconnectWaitMin = TimeSpan.FromMilliseconds(500),
            ReconnectWaitMax = TimeSpan.FromSeconds(5),
            ReconnectJitter = TimeSpan.FromMilliseconds(500),
            PublishTimeoutOnDisconnected = false
        });
        Subscribe(connection);
        return connection;
    }

    private static NatsAuthOpts CreateAuthOpts(RealtimeIntegrationAuthOptions? auth)
    {
        if (auth is null)
            return NatsAuthOpts.Default;

        if (!string.IsNullOrWhiteSpace(auth.CredsFile))
            return new NatsAuthOpts { CredsFile = auth.CredsFile };

        if (!string.IsNullOrWhiteSpace(auth.NKeyFile))
            return new NatsAuthOpts { NKeyFile = auth.NKeyFile };

        if (!string.IsNullOrWhiteSpace(auth.Seed) || !string.IsNullOrWhiteSpace(auth.NKey))
        {
            return new NatsAuthOpts
            {
                Seed = auth.Seed,
                NKey = auth.NKey
            };
        }

        if (!string.IsNullOrWhiteSpace(auth.Token))
            return new NatsAuthOpts { Token = auth.Token };

        if (!string.IsNullOrWhiteSpace(auth.Username))
        {
            return new NatsAuthOpts
            {
                Username = auth.Username,
                Password = auth.Password
            };
        }

        return NatsAuthOpts.Default;
    }

    private void Subscribe(INatsConnection connection)
    {
        connection.ConnectionOpened += OnConnectionOpenedAsync;
        connection.ConnectionDisconnected += OnConnectionDisconnectedAsync;
        connection.ReconnectFailed += OnReconnectFailedAsync;
        connection.MessageDropped += OnMessageDroppedAsync;
        connection.SlowConsumerDetected += OnSlowConsumerDetectedAsync;
        connection.ServerError += OnServerErrorAsync;
    }

    private void Unsubscribe(INatsConnection connection)
    {
        connection.ConnectionOpened -= OnConnectionOpenedAsync;
        connection.ConnectionDisconnected -= OnConnectionDisconnectedAsync;
        connection.ReconnectFailed -= OnReconnectFailedAsync;
        connection.MessageDropped -= OnMessageDroppedAsync;
        connection.SlowConsumerDetected -= OnSlowConsumerDetectedAsync;
        connection.ServerError -= OnServerErrorAsync;
    }

    private ValueTask OnConnectionOpenedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordConnectionOpened();
        _logger.LogInformation("实时集成 NATS 连接已建立。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnConnectionDisconnectedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordConnectionDisconnected();
        _logger.LogWarning("实时集成 NATS 连接已断开，将自动重连。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnReconnectFailedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordReconnectFailure();
        _logger.LogDebug("实时集成 NATS 重连尝试失败。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnMessageDroppedAsync(object? sender, NatsMessageDroppedEventArgs args)
    {
        _metrics.RecordMessageDropped(args.Subject, args.Pending);
        _logger.LogError(
            "实时集成 NATS 本地订阅消息被丢弃。Subject={Subject}；Pending={Pending}",
            args.Subject,
            args.Pending);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnSlowConsumerDetectedAsync(object? sender, NatsSlowConsumerEventArgs args)
    {
        _metrics.RecordSlowConsumer();
        _logger.LogWarning("实时集成 NATS 检测到慢消费者。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnServerErrorAsync(object? sender, NatsServerErrorEventArgs args)
    {
        _metrics.RecordServerError(args.Kind.ToString());
        _logger.LogWarning(
            "实时集成 NATS 服务端返回错误。类型={Kind}；错误={Error}",
            args.Kind,
            args.Error);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (_client.IsValueCreated)
        {
            Unsubscribe(_client.Value);
            await _client.Value.DisposeAsync().ConfigureAwait(false);
        }
    }
}

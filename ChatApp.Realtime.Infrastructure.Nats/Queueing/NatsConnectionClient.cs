using ChatApp.Realtime.Abstractions.Diagnostics;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NATS.Client.Core;
using NATS.Net;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsConnectionClient : IAsyncDisposable
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsAuthOptions _auth;
    private readonly NatsTransportMetrics _metrics;
    private readonly ILogger<NatsConnectionClient> _logger;
    private readonly Lazy<NatsClient> _client;

    public NatsConnectionClient(
        RealtimeQueueOptions options,
        IOptions<NatsOptions> natsOptions,
        NatsTransportMetrics metrics,
        ILogger<NatsConnectionClient> logger)
    {
        _options = options;
        _auth = natsOptions.Value.Auth ?? new NatsAuthOptions();
        _metrics = metrics;
        _logger = logger;
        _client = new Lazy<NatsClient>(CreateClient);
    }

    public NatsClient Client => _client.Value;

    public async Task<TimeSpan> PingAsync(CancellationToken ct = default) =>
        await Client.PingAsync(ct).ConfigureAwait(false);

    private NatsClient CreateClient()
    {
        if (string.IsNullOrWhiteSpace(_options.Endpoint))
        {
            throw new InvalidOperationException("Nats:Url 未配置，无法创建 NATS 客户端。");
        }

        _logger.LogInformation(
            "正在创建 NATS 客户端。地址={Url}",
            NatsEndpointRedactor.ForLog(_options.Endpoint));

        var client = new NatsClient(new NatsOpts
        {
            Url = _options.Endpoint,
            Name = _options.ConsumerGroup,
            AuthOpts = CreateAuthOpts(_auth),
            ConnectTimeout = TimeSpan.FromSeconds(5),
            RequestTimeout = TimeSpan.FromSeconds(5),
            CommandTimeout = TimeSpan.FromSeconds(5),
            PingInterval = TimeSpan.FromSeconds(20),
            MaxPingOut = 2,
            MaxReconnectRetry = -1,
            ReconnectWaitMin = TimeSpan.FromMilliseconds(500),
            ReconnectWaitMax = TimeSpan.FromSeconds(5),
            ReconnectJitter = TimeSpan.FromMilliseconds(500),
            PublishTimeoutOnDisconnected = true
        });
        Subscribe(client.Connection);
        return client;
    }

    private static NatsAuthOpts CreateAuthOpts(NatsAuthOptions auth)
    {
        if (!string.IsNullOrWhiteSpace(auth.CredsFile))
        {
            return new NatsAuthOpts { CredsFile = auth.CredsFile };
        }

        if (!string.IsNullOrWhiteSpace(auth.NKeyFile))
        {
            return new NatsAuthOpts { NKeyFile = auth.NKeyFile };
        }

        if (!string.IsNullOrWhiteSpace(auth.Seed) || !string.IsNullOrWhiteSpace(auth.NKey))
        {
            return new NatsAuthOpts
            {
                Seed = auth.Seed,
                NKey = auth.NKey
            };
        }

        if (!string.IsNullOrWhiteSpace(auth.Token))
        {
            return new NatsAuthOpts { Token = auth.Token };
        }

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
        _logger.LogInformation("NATS 连接已建立。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnConnectionDisconnectedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordConnectionDisconnected();
        _logger.LogWarning("NATS 连接已断开，将自动重连。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnReconnectFailedAsync(object? sender, NatsEventArgs args)
    {
        _metrics.RecordReconnectFailure();
        _logger.LogDebug("NATS 重连尝试失败。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnMessageDroppedAsync(
        object? sender,
        NatsMessageDroppedEventArgs args)
    {
        _metrics.RecordMessageDropped(args.Subject, args.Pending);
        _logger.LogError(
            "NATS 本地订阅消息被丢弃。Subject={Subject}；Pending={Pending}",
            args.Subject,
            args.Pending);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnSlowConsumerDetectedAsync(
        object? sender,
        NatsSlowConsumerEventArgs args)
    {
        _metrics.RecordSlowConsumer();
        _logger.LogWarning("NATS 检测到慢消费者。详情={Message}", args.Message);
        return ValueTask.CompletedTask;
    }

    private ValueTask OnServerErrorAsync(object? sender, NatsServerErrorEventArgs args)
    {
        _metrics.RecordServerError(args.Kind.ToString());
        _logger.LogWarning(
            "NATS 服务端返回错误。类型={Kind}；错误={Error}",
            args.Kind,
            args.Error);
        return ValueTask.CompletedTask;
    }

    public async ValueTask DisposeAsync()
    {
        if (!_client.IsValueCreated)
            return;

        Unsubscribe(_client.Value.Connection);
        await _client.Value.DisposeAsync().ConfigureAwait(false);
    }
}

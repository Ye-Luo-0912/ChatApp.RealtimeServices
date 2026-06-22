using ChatApp.Realtime.Abstractions.Queueing;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Net;

namespace ChatApp.Realtime.Infrastructure.Nats.Queueing;

public sealed class NatsConnectionClient : IAsyncDisposable
{
    private readonly RealtimeQueueOptions _options;
    private readonly ILogger<NatsConnectionClient> _logger;
    private readonly Lazy<NatsClient> _client;

    public NatsConnectionClient(
        RealtimeQueueOptions options,
        ILogger<NatsConnectionClient> logger)
    {
        _options = options;
        _logger = logger;
        _client = new Lazy<NatsClient>(CreateClient);
    }

    public NatsClient Client => _client.Value;

    private NatsClient CreateClient()
    {
        if (string.IsNullOrWhiteSpace(_options.Endpoint))
        {
            throw new InvalidOperationException("Nats:Url 未配置，无法创建 NATS 客户端。");
        }

        _logger.LogInformation("正在创建 NATS 客户端。地址={Url}", _options.Endpoint);

        return new NatsClient(new NatsOpts
        {
            Url = _options.Endpoint
        });
    }

    public async ValueTask DisposeAsync()
    {
        if (_client.IsValueCreated)
        {
            await _client.Value.DisposeAsync().ConfigureAwait(false);
        }
    }
}

using ChatApp.Realtime.Abstractions.Queueing;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Net;

namespace ChatApp.Realtime.Infrastructure.Queueing;

/// <summary>
/// NATS 连接客户端。
/// 当前作为轻量队列入口复用单个连接；后续如果切 JetStream，可以继续在这里集中创建 JetStream 上下文。
/// </summary>
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

    /// <summary>
    /// 创建并返回一个新的 NATS 客户端实例。
    /// 该方法用于初始化与 NATS 服务器的连接，基于配置文件中的 Endpoint 属性来指定连接地址。
    /// 如果 Endpoint 未配置或为空白，则抛出 InvalidOperationException 异常。
    /// </summary>
    /// <returns>返回一个 NatsClient 对象，代表与 NATS 服务器的连接。</returns>
    /// <exception cref="InvalidOperationException">当 _options.Endpoint 为空或空白时抛出此异常，指示无法创建客户端。</exception>
    /// <remarks>
    /// 方法内部使用了日志记录器来记录尝试创建 NATS 客户端的行为及其使用的地址。
    /// </remarks>
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

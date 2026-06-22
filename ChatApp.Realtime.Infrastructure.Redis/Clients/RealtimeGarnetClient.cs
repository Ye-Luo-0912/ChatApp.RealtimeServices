using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Redis.Clients;

public sealed class RealtimeGarnetClient : IDisposable
{
    private readonly string _connectionString;
    private readonly ILogger<RealtimeGarnetClient> _logger;
    private readonly Lazy<IConnectionMultiplexer> _connection;

    public RealtimeGarnetClient(
        string connectionString,
        ILogger<RealtimeGarnetClient> logger)
    {
        _connectionString = string.IsNullOrWhiteSpace(connectionString)
            ? throw new InvalidOperationException("Garnet 连接字符串未配置。")
            : connectionString.Trim();

        _logger = logger;
        _connection = new Lazy<IConnectionMultiplexer>(Connect);
    }

    public bool IsConfigured => !string.IsNullOrWhiteSpace(_connectionString);

    public IConnectionMultiplexer GetConnection()
    {
        return _connection.Value;
    }

    public IDatabase GetDatabase()
    {
        return GetConnection().GetDatabase();
    }

    private ConnectionMultiplexer Connect()
    {
        _logger.LogInformation("正在建立 Garnet 连接。");
        return ConnectionMultiplexer.Connect(_connectionString);
    }

    public void Dispose()
    {
        if (_connection.IsValueCreated)
        {
            _connection.Value.Dispose();
        }
    }
}

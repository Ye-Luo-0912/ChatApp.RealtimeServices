using Microsoft.Extensions.Logging;
using StackExchange.Redis;

namespace ChatApp.Realtime.Infrastructure.Clients;

/// <summary>
/// RealtimeGarnetClient 类提供了与 Garnet 实时数据库的连接功能。它负责管理到 Redis 服务器的持久连接，并提供获取数据库实例的方法。
/// </summary>
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

    /// <summary>
    /// 建立与 Garnet 实时数据库的连接。
    /// </summary>
    /// <returns>返回一个表示到 Redis 服务器的持久连接的 ConnectionMultiplexer 对象。</returns>
    /// <remarks>此方法会在内部日志中记录一条信息，指示正在建立连接。如果连接字符串无效或未配置，则抛出异常的情况在构造函数中已经处理。</remarks>
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

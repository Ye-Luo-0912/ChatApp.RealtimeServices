using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Clients;

/// <summary>
/// 实时数据库客户端，用于与实时数据库进行交互。该类实现了IAsyncDisposable接口，支持异步资源释放。
/// </summary>
public sealed class RealtimeDatabaseClient : IAsyncDisposable
{
    private readonly string? _connectionString;
    private readonly ILogger<RealtimeDatabaseClient> _logger;
    private readonly Lazy<NpgsqlDataSource?> _dataSource;

    public RealtimeDatabaseClient(
        string? connectionString,
        ILogger<RealtimeDatabaseClient> logger)
    {
        _connectionString = string.IsNullOrWhiteSpace(connectionString) ? null : connectionString.Trim();
        _logger = logger;
        _dataSource = new Lazy<NpgsqlDataSource?>(CreateDataSource);
    }

    public bool IsConfigured => _connectionString is not null;

    /// <summary>
    /// 获取用于连接到实时数据库的数据源。
    /// </summary>
    /// <returns>返回一个NpgsqlDataSource实例，该实例可以用来打开与数据库的连接。</returns>
    /// <exception cref="InvalidOperationException">如果实时数据库连接字符串未配置，则抛出此异常。</exception>
    /// <remarks>确保在调用此方法之前已经正确配置了数据库连接字符串。</remarks>
    /// <seealso cref="NpgsqlDataSource"/> 用于管理与PostgreSQL数据库的连接。
    public NpgsqlDataSource GetDataSource()
    {
        return !IsConfigured ? throw new InvalidOperationException("实时数据库连接字符串未配置。") : _dataSource.Value!;
    }

    private NpgsqlDataSource? CreateDataSource()
    {
        if (_connectionString is null)
        {
            _logger.LogWarning("实时数据库连接字符串未配置，数据库客户端不会建立连接。");
            return null;
        }

        _logger.LogInformation("正在创建实时数据库数据源。");
        return new NpgsqlDataSourceBuilder(_connectionString).Build();
    }

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is { IsValueCreated: true, Value: not null })
        {
            await _dataSource.Value.DisposeAsync().ConfigureAwait(false);
        }
    }
}

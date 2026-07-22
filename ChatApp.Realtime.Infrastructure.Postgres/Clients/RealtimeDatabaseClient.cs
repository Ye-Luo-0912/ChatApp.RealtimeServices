using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Clients;

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

    public NpgsqlDataSource GetDataSource()
    {
        return !IsConfigured ? throw new InvalidOperationException("实时数据库连接字符串未配置。") : _dataSource.Value!;
    }

    public async Task PingAsync(CancellationToken ct = default)
    {
        await using var connection = await GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand("SELECT 1", connection);
        await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
    }

    private NpgsqlDataSource? CreateDataSource()
    {
        if (_connectionString is null)
        {
            _logger.LogWarning("实时数据库连接字符串未配置，数据库客户端不会建立连接。");
            return null;
        }

        _logger.LogInformation("正在创建实时数据库数据源。");
        var connectionOptions = new NpgsqlConnectionStringBuilder(_connectionString);
        if (connectionOptions.MaxAutoPrepare == 0)
        {
            connectionOptions.MaxAutoPrepare = 50;
            connectionOptions.AutoPrepareMinUsages = 2;
        }

        return new NpgsqlDataSourceBuilder(connectionOptions.ConnectionString).Build();
    }

    public async ValueTask DisposeAsync()
    {
        if (_dataSource is { IsValueCreated: true, Value: not null })
        {
            await _dataSource.Value.DisposeAsync().ConfigureAwait(false);
        }
    }
}

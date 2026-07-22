using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace ChatApp.Realtime.Infrastructure.Postgres.Initialization;

public sealed class RealtimeDatabaseInitializer : IHostedService
{
    private const int MaxInitializeAttempts = 30;
    private static readonly TimeSpan RetryDelay = TimeSpan.FromSeconds(1);

    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly IOptions<RealtimeDatabaseOptions> _databaseOptions;
    private readonly ILogger<RealtimeDatabaseInitializer> _logger;

    public RealtimeDatabaseInitializer(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        IOptions<RealtimeDatabaseOptions> databaseOptions,
        ILogger<RealtimeDatabaseInitializer> logger)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _databaseOptions = databaseOptions;
        _logger = logger;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        for (var attempt = 1; ; attempt++)
        {
            try
            {
                await InitializeAsync(cancellationToken).ConfigureAwait(false);
                return;
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                throw;
            }
            catch (Exception ex)
            {
                if (attempt >= MaxInitializeAttempts)
                {
                    _logger.LogError(
                        ex,
                        "实时数据库初始化失败，已达到最大重试次数。尝试次数={Attempt}",
                        attempt);
                    throw;
                }

                _logger.LogWarning(
                    ex,
                    "实时数据库初始化失败，将在短暂等待后重试。尝试次数={Attempt}/{MaxAttempts}；等待毫秒={DelayMs}",
                    attempt,
                    MaxInitializeAttempts,
                    RetryDelay.TotalMilliseconds);

                await Task.Delay(RetryDelay, cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private async Task InitializeAsync(CancellationToken cancellationToken)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation(
            "正在通过版本化迁移初始化实时数据库。数据库架构={Schema}",
            _databaseOptions.Value.Schema);

        var runner = new RealtimeSchemaMigrationRunner(_databaseSchema, _logger);
        await runner.MigrateAsync(connection, cancellationToken).ConfigureAwait(false);

        _logger.LogInformation(
            "实时数据库版本化迁移完成。数据库架构={Schema}",
            _databaseOptions.Value.Schema);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Configuration;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

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
        var schema = _databaseSchema.QuotedSchema;
        var table = _databaseSchema.MessagesTableSql;

        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(cancellationToken)
            .ConfigureAwait(false);

        _logger.LogInformation("正在初始化实时数据库最小表结构。数据库架构={Schema}", _databaseOptions.Value.Schema);

        var commands = new[]
        {
            $"CREATE SCHEMA IF NOT EXISTS {schema};",
            $"""
             CREATE TABLE IF NOT EXISTS {table} (
                 "message_id" character varying(64) NOT NULL PRIMARY KEY,
                 "client_message_id" character varying(128) NOT NULL,
                 "sender_user_id" bigint NOT NULL,
                 "sender_session_id" character varying(128) NOT NULL,
                 "receiver_user_id" bigint NOT NULL,
                 "content" text NOT NULL,
                 "received_at_ms" bigint NOT NULL,
                 "created_at_ms" bigint NOT NULL
             );
             """,
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_client_message_id\" ON {table} (\"client_message_id\");",
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_sender_user_id\" ON {table} (\"sender_user_id\");",
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_receiver_user_id\" ON {table} (\"receiver_user_id\");",
            $"CREATE INDEX IF NOT EXISTS \"ix_messages_received_at_ms\" ON {table} (\"received_at_ms\");",
            $"CREATE UNIQUE INDEX IF NOT EXISTS \"ux_messages_sender_client_message\" ON {table} (\"sender_user_id\", \"client_message_id\");"
        };

        foreach (var command in commands)
        {
            await using var dbCommand = new NpgsqlCommand(command, connection);
            await dbCommand.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
        }

        _logger.LogInformation("实时数据库最小表结构初始化完成。数据库架构={Schema}", _databaseOptions.Value.Schema);
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        return Task.CompletedTask;
    }
}

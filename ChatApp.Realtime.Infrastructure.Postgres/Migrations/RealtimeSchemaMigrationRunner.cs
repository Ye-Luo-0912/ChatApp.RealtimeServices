using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public sealed class RealtimeSchemaMigrationRunner
{
    private readonly RealtimeDatabaseSchema _schema;
    private readonly ILogger _logger;
    private readonly IReadOnlyList<IRealtimeSchemaMigration> _migrations;

    public RealtimeSchemaMigrationRunner(
        RealtimeDatabaseSchema schema,
        ILogger logger,
        IEnumerable<IRealtimeSchemaMigration>? migrations = null)
    {
        _schema = schema;
        _logger = logger;
        _migrations = (migrations ?? DefaultMigrations())
            .OrderBy(item => item.Version)
            .ToArray();

        ValidateMigrationVersions(_migrations);
    }

    public static IReadOnlyList<IRealtimeSchemaMigration> DefaultMigrations() =>
    [
        new Migration001_BaselineSchema(),
        new Migration002_OutboxTypedTargetColumns()
    ];

    public async Task MigrateAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken = default)
    {
        await EnsureMigrationsTableAsync(connection, cancellationToken).ConfigureAwait(false);

        foreach (var migration in _migrations)
        {
            if (await IsAppliedAsync(connection, migration.Version, cancellationToken).ConfigureAwait(false))
            {
                _logger.LogDebug(
                    "实时库迁移已应用，跳过。版本={Version}；名称={Name}",
                    migration.Version,
                    migration.Name);
                continue;
            }

            _logger.LogInformation(
                "正在应用实时库迁移。版本={Version}；名称={Name}",
                migration.Version,
                migration.Name);

            await using var transaction = await connection
                .BeginTransactionAsync(cancellationToken)
                .ConfigureAwait(false);

            await migration
                .ApplyAsync(connection, transaction, _schema, cancellationToken)
                .ConfigureAwait(false);

            await RecordAppliedAsync(
                    connection,
                    transaction,
                    migration,
                    cancellationToken)
                .ConfigureAwait(false);

            await transaction.CommitAsync(cancellationToken).ConfigureAwait(false);

            _logger.LogInformation(
                "实时库迁移已完成。版本={Version}；名称={Name}",
                migration.Version,
                migration.Name);
        }
    }

    private async Task EnsureMigrationsTableAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken)
    {
        await using var createSchema = new NpgsqlCommand(
            $"CREATE SCHEMA IF NOT EXISTS {_schema.QuotedSchema};",
            connection);
        await createSchema.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);

        await using var createTable = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {_schema.SchemaMigrationsTableSql} (
                 "version" integer NOT NULL PRIMARY KEY,
                 "name" character varying(128) NOT NULL,
                 "applied_at_ms" bigint NOT NULL
             );
             """,
            connection);
        await createTable.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task<bool> IsAppliedAsync(
        NpgsqlConnection connection,
        int version,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT 1 FROM {_schema.SchemaMigrationsTableSql} WHERE \"version\" = @version",
            connection);
        command.Parameters.AddWithValue("version", version);
        var result = await command.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
        return result is not null;
    }

    private async Task RecordAppliedAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        IRealtimeSchemaMigration migration,
        CancellationToken cancellationToken)
    {
        await using var command = new NpgsqlCommand(
            $"""
             INSERT INTO {_schema.SchemaMigrationsTableSql} ("version", "name", "applied_at_ms")
             VALUES (@version, @name, @applied_at_ms);
             """,
            connection,
            transaction);
        command.Parameters.AddWithValue("version", migration.Version);
        command.Parameters.AddWithValue("name", migration.Name);
        command.Parameters.AddWithValue(
            "applied_at_ms",
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
    }

    private static void ValidateMigrationVersions(IReadOnlyList<IRealtimeSchemaMigration> migrations)
    {
        var versions = new HashSet<int>();
        foreach (var migration in migrations)
        {
            if (migration.Version <= 0)
                throw new InvalidOperationException($"迁移版本必须为正整数：{migration.Name}");
            if (!versions.Add(migration.Version))
                throw new InvalidOperationException($"迁移版本重复：{migration.Version}");
        }
    }
}

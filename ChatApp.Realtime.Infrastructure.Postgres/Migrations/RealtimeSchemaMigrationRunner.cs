using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Migrations;

public sealed class RealtimeSchemaMigrationRunner
{
    /// <summary>固定会话级 advisory lock key，防止并发 MigrateAsync。</summary>
    public const long AdvisoryLockKey = 0x5245_414C_5449_4D45L; // "REALTIME"

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
        new Migration002_OutboxTypedTargetColumns(),
        new Migration003_OutboxLifecycle(),
        new Migration004_MessageContentFingerprint(),
        new Migration005_ConversationFoundation(),
        new Migration006_ConversationListIndex(),
        new Migration007_ConversationMemberPrefs(),
        new Migration008_DeviceSyncCursors(),
        new Migration009_ConversationBackfillBatches(),
        new Migration010_ConversationListMemberIndex(),
        new Migration011_OutboxStatsPartialIndexes(),
        new Migration012_Attachments(),
        new Migration013_MessageReply(),
        new Migration014_MessageRecall(),
        new Migration015_MessageForward(),
        new Migration016_AttachmentContentHash(),
        new Migration017_MessageEditAndChangeWatermark()
    ];

    public async Task MigrateAsync(
        NpgsqlConnection connection,
        CancellationToken cancellationToken = default)
    {
        var lockAcquired = false;
        for (var attempt = 0; attempt < 120; attempt++)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await using (var lockCommand = new NpgsqlCommand(
                               "SELECT pg_try_advisory_lock(@key);",
                               connection))
            {
                lockCommand.Parameters.AddWithValue("key", AdvisoryLockKey);
                var acquired = await lockCommand.ExecuteScalarAsync(cancellationToken).ConfigureAwait(false);
                if (acquired is true)
                {
                    lockAcquired = true;
                    break;
                }
            }

            await Task.Delay(50, cancellationToken).ConfigureAwait(false);
        }

        if (!lockAcquired)
        {
            throw new InvalidOperationException(
                "无法获取实时库迁移 advisory lock（另一实例可能正在迁移）。");
        }

        try
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

                if (migration.RequiresTransaction)
                {
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
                }
                else
                {
                    try
                    {
                        await migration
                            .ApplyAsync(connection, null, _schema, cancellationToken)
                            .ConfigureAwait(false);
                    }
                    catch (RealtimeMigrationDeferredException deferred)
                    {
                        _logger.LogWarning(
                            "实时库迁移暂停（未记为已应用）。版本={Version}；名称={Name}；原因={Reason}",
                            deferred.Version,
                            deferred.Name,
                            deferred.Message);
                        return;
                    }

                    await RecordAppliedAsync(
                            connection,
                            null,
                            migration,
                            cancellationToken)
                        .ConfigureAwait(false);
                }

                _logger.LogInformation(
                    "实时库迁移已完成。版本={Version}；名称={Name}",
                    migration.Version,
                    migration.Name);
            }
        }
        finally
        {
            await using var unlockCommand = new NpgsqlCommand(
                "SELECT pg_advisory_unlock(@key);",
                connection);
            unlockCommand.Parameters.AddWithValue("key", AdvisoryLockKey);
            await unlockCommand.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
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

        await using var createCheckpoints = new NpgsqlCommand(
            $"""
             CREATE TABLE IF NOT EXISTS {_schema.SchemaMigrationCheckpointsTableSql} (
                 "migration_version" integer NOT NULL,
                 "phase" character varying(64) NOT NULL,
                 "checkpoint_key" character varying(128) NOT NULL DEFAULT '',
                 "checkpoint_value" text NULL,
                 "updated_at_ms" bigint NOT NULL,
                 PRIMARY KEY ("migration_version", "phase", "checkpoint_key")
             );
             """,
            connection);
        await createCheckpoints.ExecuteNonQueryAsync(cancellationToken).ConfigureAwait(false);
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
        NpgsqlTransaction? transaction,
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

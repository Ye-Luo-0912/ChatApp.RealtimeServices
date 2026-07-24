using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeOpsQueryStore(
    RealtimeDatabaseClient client,
    RealtimeDatabaseSchema schema,
    IRealtimeOutboxStore outboxStore,
    ILogger<NpgsqlRealtimeOpsQueryStore> logger,
    IOptions<MessageRetentionOptions>? messageRetentionOptions = null,
    SyncBootstrapOptions? syncBootstrapOptions = null,
    IRealtimeMessageRetentionStore? messageRetentionStore = null) : IRealtimeOpsQueryStore
{
    private static readonly IReadOnlyList<RealtimeMigrationCatalogEntryDto> Catalog =
        RealtimeSchemaMigrationRunner.DefaultMigrations()
            .Select(m => new RealtimeMigrationCatalogEntryDto(m.Version, m.Name))
            .ToArray();

    public async Task<RealtimeMigrationProgressDto> GetMigrationProgressAsync(
        CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        if (!client.IsConfigured)
        {
            return new RealtimeMigrationProgressDto(
                Catalog, [], [], Catalog.Select(c => c.Version).ToArray(), true, now);
        }

        await using var connection = await client.GetDataSource().OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        var applied = await ReadAppliedAsync(connection, ct).ConfigureAwait(false);
        var appliedVersions = applied.Select(a => a.Version).ToHashSet();
        var checkpoints = await ReadCheckpointsAsync(connection, ct).ConfigureAwait(false);

        // Open = checkpoint rows whose migration version is not yet recorded as applied.
        var open = checkpoints
            .Where(c => !appliedVersions.Contains(c.MigrationVersion))
            .OrderBy(c => c.MigrationVersion)
            .ThenBy(c => c.Phase, StringComparer.Ordinal)
            .ToList();

        var notFullyApplied = Catalog
            .Select(c => c.Version)
            .Where(v => !appliedVersions.Contains(v))
            .ToList();

        var hasDeferred = open.Count > 0
            || notFullyApplied.Contains(9)
            || notFullyApplied.Contains(10);

        return new RealtimeMigrationProgressDto(
            Catalog,
            applied,
            open,
            notFullyApplied,
            hasDeferred,
            now);
    }

    public async Task<RealtimeOpsBacklogDto> GetBacklogsAsync(CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var outbox = await outboxStore.GetStatsAsync(ct).ConfigureAwait(false);
        long? oldestAge = outbox.OldestPendingAtMs is { } oldest
            ? Math.Max(0, now - oldest)
            : null;

        var mig009Applied = false;
        long missingConversationId = 0;
        var attachmentsAvailable = false;
        long ticketed = 0, confirmedUnbound = 0, scanning = 0, abandoned = 0;
        long? beyondRetention = null;
        long? oldestPurgeable = null;

        var retention = messageRetentionOptions?.Value ?? new MessageRetentionOptions();
        var syncHorizon = syncBootstrapOptions?.RetentionHorizonMs ?? 0;
        var horizonMs = retention.ResolveEffectiveHorizonMs(syncHorizon);
        var retentionActive = retention.IsEffectivelyEnabled(syncHorizon);

        if (client.IsConfigured)
        {
            await using var connection = await client.GetDataSource().OpenConnectionAsync(ct)
                .ConfigureAwait(false);

            mig009Applied = await IsAppliedAsync(connection, 9, ct).ConfigureAwait(false);
            if (!mig009Applied)
            {
                missingConversationId = await CountAsync(
                        connection,
                        $"""
                         SELECT COUNT(*)::bigint
                         FROM {schema.MessagesTableSql}
                         WHERE conversation_id IS NULL
                         """,
                        ct)
                    .ConfigureAwait(false);
            }

            try
            {
                await using var cmd = new NpgsqlCommand(
                    $"""
                     SELECT
                       COUNT(*) FILTER (WHERE status = 0)::bigint AS ticketed,
                       COUNT(*) FILTER (WHERE status = 1 AND message_id IS NULL)::bigint AS confirmed_unbound,
                       COUNT(*) FILTER (WHERE status = 5)::bigint AS scanning,
                       COUNT(*) FILTER (WHERE status = 3)::bigint AS abandoned
                     FROM {schema.AttachmentsTableSql}
                     """,
                    connection);
                await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
                if (await reader.ReadAsync(ct).ConfigureAwait(false))
                {
                    attachmentsAvailable = true;
                    ticketed = reader.GetInt64(0);
                    confirmedUnbound = reader.GetInt64(1);
                    scanning = reader.GetInt64(2);
                    abandoned = reader.GetInt64(3);
                }
            }
            catch (PostgresException ex) when (ex.SqlState is "42P01" or "42703")
            {
                logger.LogDebug(ex, "attachments 表不可用，跳过 ops backlog 附件计数");
            }

            if (retentionActive && messageRetentionStore is not null)
            {
                try
                {
                    var cutoff = now - horizonMs;
                    if (cutoff > 0)
                    {
                        var stats = await messageRetentionStore
                            .GetPurgeableStatsAsync(cutoff, ct)
                            .ConfigureAwait(false);
                        beyondRetention = stats.PurgeableCount;
                        oldestPurgeable = stats.OldestReceivedAtMs;
                    }
                    else
                    {
                        beyondRetention = 0;
                    }
                }
                catch (Exception ex)
                {
                    logger.LogDebug(ex, "message retention purgeable count failed");
                }
            }
        }

        return new RealtimeOpsBacklogDto(
            OutboxPendingCount: outbox.PendingCount,
            OutboxDeadCount: outbox.DeadCount,
            OldestOutboxPendingAtMs: outbox.OldestPendingAtMs,
            OldestOutboxPendingAgeMs: oldestAge,
            OutboxMaxAttemptCount: outbox.MaxAttemptCount,
            Migration009Applied: mig009Applied,
            MessagesMissingConversationIdCount: missingConversationId,
            AttachmentsTableAvailable: attachmentsAvailable,
            AttachmentTicketedCount: ticketed,
            AttachmentConfirmedUnboundCount: confirmedUnbound,
            AttachmentScanningCount: scanning,
            AttachmentAbandonedCount: abandoned,
            MessagesBeyondRetentionCount: beyondRetention,
            OldestPurgeableReceivedAtMs: oldestPurgeable,
            CleanupNote:
            "Account cleanup saga / inbox DLQ / blob delete jobs live on ChatApp.Server (/api/admin/account-cleanup-saga, /api/admin/ops).",
            GeneratedAtMs: now);
    }

    private async Task<IReadOnlyList<RealtimeAppliedMigrationDto>> ReadAppliedAsync(
        NpgsqlConnection connection,
        CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                $"""
                 SELECT version, name, applied_at_ms
                 FROM {schema.SchemaMigrationsTableSql}
                 ORDER BY version
                 """,
                connection);
            var rows = new List<RealtimeAppliedMigrationDto>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(new RealtimeAppliedMigrationDto(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.GetInt64(2)));
            }

            return rows;
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01")
        {
            logger.LogDebug(ex, "schema_migrations 不存在");
            return [];
        }
    }

    private async Task<IReadOnlyList<RealtimeMigrationCheckpointDto>> ReadCheckpointsAsync(
        NpgsqlConnection connection,
        CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                $"""
                 SELECT migration_version, phase, checkpoint_key, checkpoint_value, updated_at_ms
                 FROM {schema.SchemaMigrationCheckpointsTableSql}
                 ORDER BY migration_version, phase, checkpoint_key
                 """,
                connection);
            var rows = new List<RealtimeMigrationCheckpointDto>();
            await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                rows.Add(new RealtimeMigrationCheckpointDto(
                    reader.GetInt32(0),
                    reader.GetString(1),
                    reader.IsDBNull(2) ? "" : reader.GetString(2),
                    reader.IsDBNull(3) ? null : reader.GetString(3),
                    reader.GetInt64(4)));
            }

            return rows;
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01")
        {
            logger.LogDebug(ex, "schema_migration_checkpoints 不存在");
            return [];
        }
    }

    private async Task<bool> IsAppliedAsync(
        NpgsqlConnection connection,
        int version,
        CancellationToken ct)
    {
        try
        {
            await using var cmd = new NpgsqlCommand(
                $"""
                 SELECT 1
                 FROM {schema.SchemaMigrationsTableSql}
                 WHERE version = @version
                 LIMIT 1;
                 """,
                connection);
            cmd.Parameters.AddWithValue("version", version);
            var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
            return result is not null;
        }
        catch (PostgresException ex) when (ex.SqlState is "42P01")
        {
            return false;
        }
    }

    private static async Task<long> CountAsync(
        NpgsqlConnection connection,
        string sql,
        CancellationToken ct)
    {
        await using var cmd = new NpgsqlCommand(sql, connection);
        var result = await cmd.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long l ? l : Convert.ToInt64(result);
    }
}

/// <summary>未配置 Postgres 时的空实现。</summary>
public sealed class NoopRealtimeOpsQueryStore : IRealtimeOpsQueryStore
{
    public static NoopRealtimeOpsQueryStore Instance { get; } = new();

    private static readonly IReadOnlyList<RealtimeMigrationCatalogEntryDto> Catalog =
        RealtimeSchemaMigrationRunner.DefaultMigrations()
            .Select(m => new RealtimeMigrationCatalogEntryDto(m.Version, m.Name))
            .ToArray();

    public Task<RealtimeMigrationProgressDto> GetMigrationProgressAsync(CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return Task.FromResult(new RealtimeMigrationProgressDto(
            Catalog,
            [],
            [],
            Catalog.Select(c => c.Version).ToArray(),
            HasDeferredInProgress: true,
            now));
    }

    public Task<RealtimeOpsBacklogDto> GetBacklogsAsync(CancellationToken ct = default)
    {
        var now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        return Task.FromResult(new RealtimeOpsBacklogDto(
            0, 0, null, null, 0,
            Migration009Applied: false,
            MessagesMissingConversationIdCount: 0,
            AttachmentsTableAvailable: false,
            AttachmentTicketedCount: 0,
            AttachmentConfirmedUnboundCount: 0,
            AttachmentScanningCount: 0,
            AttachmentAbandonedCount: 0,
            MessagesBeyondRetentionCount: null,
            OldestPurgeableReceivedAtMs: null,
            CleanupNote:
            "Account cleanup saga / inbox DLQ / blob delete jobs live on ChatApp.Server (/api/admin/account-cleanup-saga, /api/admin/ops).",
            GeneratedAtMs: now));
    }
}

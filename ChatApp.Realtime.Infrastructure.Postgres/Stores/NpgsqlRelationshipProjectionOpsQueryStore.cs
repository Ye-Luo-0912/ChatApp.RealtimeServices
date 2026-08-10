using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRelationshipProjectionOpsQueryStore(
    RealtimeDatabaseClient client,
    RealtimeDatabaseSchema schema,
    ILogger<NpgsqlRelationshipProjectionOpsQueryStore> logger)
    : IRelationshipProjectionOpsQueryStore
{
    private const int MaxPageSize = 500;

    public async Task<RelationshipProjectionOpsStatusDto> GetStatusAsync(
        CancellationToken ct = default)
    {
        if (!client.IsConfigured)
            return UnavailableStatus(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var sql = schema.GetOrAddCommandText(
            "ops-relationship-projection-status",
            static current => $"""
                WITH clock AS (
                    SELECT (extract(epoch FROM clock_timestamp()) * 1000)::bigint AS now_ms
                ), coverage AS (
                    SELECT COUNT(*)::bigint AS version_streams,
                           COUNT(*) FILTER (WHERE EXISTS (
                               SELECT 1
                               FROM {current.RelationshipProjectionSnapshotsTableSql} AS snapshot
                               WHERE snapshot."owner_user_id" = version."owner_user_id"
                                 AND snapshot."list_type" = version."list_type"
                                 AND snapshot."version" <= version."current_version"))::bigint
                               AS snapshot_streams
                    FROM {current.RelationshipProjectionVersionsTableSql} AS version
                )
                SELECT state."pass_number", state."stable_passes", state."pass_changed",
                       state."after_owner_user_id", state."after_list_type",
                       state."locked_until_ms" IS NOT NULL
                           AND state."locked_until_ms" >= clock.now_ms AS lease_active,
                       state."locked_until_ms", state."next_attempt_at_ms", state."last_error",
                       coverage.version_streams, coverage.snapshot_streams,
                       (SELECT COUNT(*)::bigint FROM {current.RelationshipProjectionItemsTableSql}),
                       (SELECT COUNT(*)::bigint FROM {current.RelationshipProjectionInboxTableSql}),
                       state."updated_at_ms", clock.now_ms
                FROM {current.RelationshipProjectionRebuildStateTableSql} AS state
                CROSS JOIN clock
                CROSS JOIN coverage
                WHERE state."id" = 1;
                """);
        try
        {
            await using var connection = await client.GetDataSource().OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            await using var command = new NpgsqlCommand(sql, connection);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            if (!await reader.ReadAsync(ct).ConfigureAwait(false))
                return UnavailableStatus(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

            var versionStreams = reader.GetInt64(9);
            var snapshotStreams = reader.GetInt64(10);
            return new RelationshipProjectionOpsStatusDto(
                Available: true,
                PassNumber: reader.GetInt64(0),
                StablePasses: reader.GetInt32(1),
                PassChanged: reader.GetBoolean(2),
                CursorOwnerUserId: reader.IsDBNull(3) ? null : reader.GetInt64(3),
                CursorListType: reader.IsDBNull(4)
                    ? null
                    : (RelationshipProjectionListType)reader.GetInt16(4),
                LeaseActive: reader.GetBoolean(5),
                LockedUntilMs: reader.IsDBNull(6) ? null : reader.GetInt64(6),
                NextAttemptAtMs: reader.GetInt64(7),
                LastError: reader.IsDBNull(8) ? null : reader.GetString(8),
                VersionStreamCount: versionStreams,
                SnapshotBaselineStreamCount: snapshotStreams,
                StreamsWithoutSnapshotBaselineCount: versionStreams - snapshotStreams,
                ProjectionItemCount: reader.GetInt64(11),
                InboxEventCount: reader.GetInt64(12),
                UpdatedAtMs: reader.GetInt64(13),
                GeneratedAtMs: reader.GetInt64(14));
        }
        catch (PostgresException ex) when (IsMissingSchema(ex))
        {
            logger.LogDebug(ex, "relationship projection ops tables are unavailable");
            return UnavailableStatus(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
    }

    public async Task<RelationshipProjectionOpsStreamPageDto> ListStreamsAsync(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize,
        CancellationToken ct = default)
    {
        ValidateCursor(afterOwnerUserId, afterListType, pageSize);
        if (!client.IsConfigured)
            return UnavailablePage();

        var sql = schema.GetOrAddCommandText(
            "ops-relationship-projection-streams",
            static current => $"""
                WITH selected AS (
                    SELECT version."owner_user_id", version."list_type",
                           version."current_version", version."updated_at_ms"
                    FROM {current.RelationshipProjectionVersionsTableSql} AS version
                    WHERE @after_owner_user_id IS NULL
                       OR (version."owner_user_id", version."list_type")
                          > (@after_owner_user_id, @after_list_type)
                    ORDER BY version."owner_user_id", version."list_type"
                    LIMIT @limit
                )
                SELECT selected."owner_user_id", selected."list_type",
                       selected."current_version", selected."updated_at_ms",
                       COALESCE(items.item_count, 0)::bigint,
                       snapshot."version", snapshot."item_count", snapshot."resource_hash",
                       COALESCE(inbox.delta_count, 0)::bigint, inbox.max_version,
                       snapshot."version" IS NOT NULL AS has_snapshot_baseline,
                       COALESCE(inbox.delta_count, 0)
                           = selected."current_version" - COALESCE(snapshot."version", 0)
                           AS is_locally_contiguous
                FROM selected
                LEFT JOIN LATERAL (
                    SELECT COUNT(*)::bigint AS item_count
                    FROM {current.RelationshipProjectionItemsTableSql} AS item
                    WHERE item."owner_user_id" = selected."owner_user_id"
                      AND item."list_type" = selected."list_type"
                ) AS items ON TRUE
                LEFT JOIN LATERAL (
                    SELECT checkpoint."version", checkpoint."item_count",
                           checkpoint."resource_hash"
                    FROM {current.RelationshipProjectionSnapshotsTableSql} AS checkpoint
                    WHERE checkpoint."owner_user_id" = selected."owner_user_id"
                      AND checkpoint."list_type" = selected."list_type"
                      AND checkpoint."version" <= selected."current_version"
                    ORDER BY checkpoint."version" DESC, checkpoint."applied_at_ms" DESC
                    LIMIT 1
                ) AS snapshot ON TRUE
                LEFT JOIN LATERAL (
                    SELECT COUNT(*)::bigint AS delta_count,
                           MAX(event."version")::bigint AS max_version
                    FROM {current.RelationshipProjectionInboxTableSql} AS event
                    WHERE event."owner_user_id" = selected."owner_user_id"
                      AND event."list_type" = selected."list_type"
                      AND event."version" > COALESCE(snapshot."version", 0)
                      AND event."version" <= selected."current_version"
                ) AS inbox ON TRUE
                ORDER BY selected."owner_user_id", selected."list_type";
                """);
        try
        {
            await using var connection = await client.GetDataSource().OpenConnectionAsync(ct)
                .ConfigureAwait(false);
            await using var command = new NpgsqlCommand(sql, connection);
            command.Parameters.AddWithValue(
                "after_owner_user_id",
                NpgsqlDbType.Bigint,
                (object?)afterOwnerUserId ?? DBNull.Value);
            command.Parameters.AddWithValue(
                "after_list_type",
                NpgsqlDbType.Smallint,
                afterListType is null ? DBNull.Value : (short)afterListType.Value);
            command.Parameters.AddWithValue("limit", NpgsqlDbType.Integer, pageSize + 1);

            var items = new List<RelationshipProjectionOpsStreamDto>(pageSize + 1);
            await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
            while (await reader.ReadAsync(ct).ConfigureAwait(false))
            {
                items.Add(new RelationshipProjectionOpsStreamDto(
                    OwnerUserId: reader.GetInt64(0),
                    ListType: (RelationshipProjectionListType)reader.GetInt16(1),
                    CurrentVersion: reader.GetInt64(2),
                    UpdatedAtMs: reader.GetInt64(3),
                    CurrentItemCount: reader.GetInt64(4),
                    SnapshotVersion: reader.IsDBNull(5) ? null : reader.GetInt64(5),
                    SnapshotItemCount: reader.IsDBNull(6) ? null : reader.GetInt32(6),
                    SnapshotResourceHash: reader.IsDBNull(7) ? null : reader.GetString(7),
                    DeltaInboxCountAfterSnapshot: reader.GetInt64(8),
                    DeltaInboxMaxVersion: reader.IsDBNull(9) ? null : reader.GetInt64(9),
                    HasSnapshotBaseline: reader.GetBoolean(10),
                    IsLocallyContiguous: reader.GetBoolean(11)));
            }

            var hasMore = items.Count > pageSize;
            if (hasMore)
                items.RemoveAt(items.Count - 1);
            var last = hasMore ? items[^1] : null;
            return new RelationshipProjectionOpsStreamPageDto(
                Available: true,
                Items: items,
                HasMore: hasMore,
                NextOwnerUserId: last?.OwnerUserId,
                NextListType: last?.ListType,
                GeneratedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        }
        catch (PostgresException ex) when (IsMissingSchema(ex))
        {
            logger.LogDebug(ex, "relationship projection ops tables are unavailable");
            return UnavailablePage();
        }
    }

    private static void ValidateCursor(
        long? afterOwnerUserId,
        RelationshipProjectionListType? afterListType,
        int pageSize)
    {
        if (pageSize is < 1 or > MaxPageSize)
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (afterOwnerUserId is <= 0)
            throw new ArgumentOutOfRangeException(nameof(afterOwnerUserId));
        if (afterOwnerUserId.HasValue != afterListType.HasValue)
            throw new ArgumentException("Both relationship projection cursor fields are required.");
        if (afterListType is { } listType && !Enum.IsDefined(listType))
            throw new ArgumentOutOfRangeException(nameof(afterListType));
    }

    private static bool IsMissingSchema(PostgresException ex) =>
        ex.SqlState is "42P01" or "42703";

    private static RelationshipProjectionOpsStatusDto UnavailableStatus(long generatedAtMs) =>
        new(
            Available: false,
            PassNumber: 0,
            StablePasses: 0,
            PassChanged: false,
            CursorOwnerUserId: null,
            CursorListType: null,
            LeaseActive: false,
            LockedUntilMs: null,
            NextAttemptAtMs: 0,
            LastError: null,
            VersionStreamCount: 0,
            SnapshotBaselineStreamCount: 0,
            StreamsWithoutSnapshotBaselineCount: 0,
            ProjectionItemCount: 0,
            InboxEventCount: 0,
            UpdatedAtMs: 0,
            GeneratedAtMs: generatedAtMs);

    private static RelationshipProjectionOpsStreamPageDto UnavailablePage() =>
        new(
            Available: false,
            Items: [],
            HasMore: false,
            NextOwnerUserId: null,
            NextListType: null,
            GeneratedAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
}

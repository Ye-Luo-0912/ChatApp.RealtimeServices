using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

/// <summary>
/// Durable shadow projection for Server-owned relationship lists. A stream row is locked
/// before checking the inbox, which linearizes concurrent retries and version advances
/// without any process-local mutable state.
/// </summary>
public sealed class NpgsqlRelationshipProjectionStore : IRelationshipProjectionStore
{
    private const int MaxSnapshotItems = 100_000;
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _schema;
    private readonly RealtimeMetrics? _metrics;

    public NpgsqlRelationshipProjectionStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema schema,
        RealtimeMetrics? metrics = null)
    {
        _databaseClient = databaseClient;
        _schema = schema;
        _metrics = metrics;
    }

    public async Task<RelationshipProjectionApplyResult> ApplyAsync(
        RelationshipProjectionDelta delta,
        CancellationToken ct = default)
    {
        Validate(delta);

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            await EnsureStreamAsync(connection, transaction, delta, ct).ConfigureAwait(false);
            var currentVersion = await LockStreamAsync(connection, transaction, delta, ct)
                .ConfigureAwait(false);

            var duplicate = await TryReadInboxAsync(connection, transaction, delta.EventId, ct)
                .ConfigureAwait(false);
            if (duplicate is not null)
            {
                if (duplicate.Value.OwnerUserId != delta.OwnerUserId
                    || duplicate.Value.ListType != delta.ListType
                    || duplicate.Value.Version != delta.Version)
                {
                    throw new InvalidOperationException(
                        $"Relationship projection event id collision: {delta.EventId}.");
                }

                await transaction.CommitAsync(ct).ConfigureAwait(false);
                _metrics?.RecordRelationshipProjectionDuplicate();
                return RelationshipProjectionApplyResult.Duplicate;
            }

            if (delta.Version <= currentVersion
                && await IsCoveredBySnapshotAsync(
                        connection,
                        transaction,
                        delta,
                        ct)
                    .ConfigureAwait(false))
            {
                await transaction.CommitAsync(ct).ConfigureAwait(false);
                _metrics?.RecordRelationshipProjectionDuplicate();
                return RelationshipProjectionApplyResult.Duplicate;
            }

            var expectedVersion = checked(currentVersion + 1);
            if (delta.Version != expectedVersion)
            {
                _metrics?.RecordRelationshipProjectionGap();
                throw new RelationshipProjectionGapException(
                    delta.OwnerUserId,
                    delta.ListType,
                    expectedVersion,
                    delta.Version);
            }

            if (delta.Operation == RelationshipProjectionOperation.Upsert)
            {
                await UpsertItemAsync(connection, transaction, delta, ct).ConfigureAwait(false);
            }
            else
            {
                await DeleteItemAsync(connection, transaction, delta, ct).ConfigureAwait(false);
            }

            await InsertInboxAsync(connection, transaction, delta, ct).ConfigureAwait(false);
            var updated = await AdvanceStreamAsync(
                    connection, transaction, delta, currentVersion, ct)
                .ConfigureAwait(false);
            if (updated != 1)
                throw new InvalidOperationException("Relationship projection stream version was not advanced.");
            await InsertHistoryAsync(connection, transaction, delta, ct).ConfigureAwait(false);

            await transaction.CommitAsync(ct).ConfigureAwait(false);
            _metrics?.RecordRelationshipProjectionApplied();
            return RelationshipProjectionApplyResult.Applied;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    public async Task<RelationshipProjectionSnapshotApplyResult> ApplySnapshotAsync(
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct = default)
    {
        ValidateSnapshot(snapshot);

        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct)
            .ConfigureAwait(false);
        await using var transaction = await connection.BeginTransactionAsync(ct).ConfigureAwait(false);

        try
        {
            await EnsureSnapshotStreamAsync(connection, transaction, snapshot, ct).ConfigureAwait(false);
            var currentVersion = await LockSnapshotStreamAsync(connection, transaction, snapshot, ct)
                .ConfigureAwait(false);

            var existing = await TryReadSnapshotAsync(
                    connection, transaction, snapshot.SnapshotId, ct)
                .ConfigureAwait(false);
            if (existing is not null)
            {
                if (existing.Value.OwnerUserId != snapshot.OwnerUserId
                    || existing.Value.ListType != snapshot.ListType
                    || existing.Value.Version != snapshot.Version
                    || !string.Equals(
                        existing.Value.ResourceHash,
                        snapshot.ResourceHash,
                        StringComparison.Ordinal))
                {
                    throw new InvalidOperationException(
                        $"Relationship projection snapshot id collision: {snapshot.SnapshotId}.");
                }

                if (currentVersion > snapshot.Version
                    || currentVersion == snapshot.Version
                    && await SnapshotItemsMatchAsync(
                            connection,
                            transaction,
                            snapshot,
                            ct)
                        .ConfigureAwait(false))
                {
                    await transaction.CommitAsync(ct).ConfigureAwait(false);
                    return RelationshipProjectionSnapshotApplyResult.Duplicate;
                }
            }

            if (currentVersion > snapshot.Version)
            {
                throw new RelationshipProjectionSnapshotVersionMismatchException(
                    snapshot.OwnerUserId,
                    snapshot.ListType,
                    snapshot.Version,
                    currentVersion);
            }

            await DeleteSnapshotStreamItemsAsync(connection, transaction, snapshot, ct)
                .ConfigureAwait(false);
            if (snapshot.Items.Count > 0)
            {
                await InsertSnapshotItemsAsync(connection, transaction, snapshot, ct)
                    .ConfigureAwait(false);
            }

            if (existing is null)
            {
                await InsertSnapshotCheckpointAsync(connection, transaction, snapshot, ct)
                    .ConfigureAwait(false);
            }
            await AdvanceSnapshotStreamAsync(
                    connection,
                    transaction,
                    snapshot,
                    currentVersion,
                    ct)
                .ConfigureAwait(false);
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return RelationshipProjectionSnapshotApplyResult.Applied;
        }
        catch
        {
            await transaction.RollbackAsync(CancellationToken.None).ConfigureAwait(false);
            throw;
        }
    }

    private async Task<bool> SnapshotItemsMatchAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-verify-snapshot-items",
            static schema => $"""
                SELECT "resource_id"
                FROM {schema.RelationshipProjectionItemsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var resourceIds = new List<string>(snapshot.ItemCount);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            resourceIds.Add(reader.GetString(0));

        return resourceIds.Count == snapshot.ItemCount
               && string.Equals(
                   RelationshipProjectionSnapshotHash.Compute(resourceIds),
                   snapshot.ResourceHash,
                   StringComparison.Ordinal);
    }

    private async Task EnsureSnapshotStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-snapshot-ensure-stream",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionVersionsTableSql}
                    ("owner_user_id", "list_type", "current_version", "updated_at_ms")
                VALUES (@owner_user_id, @list_type, 0, @updated_at_ms)
                ON CONFLICT ("owner_user_id", "list_type") DO NOTHING;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        command.Parameters.AddWithValue("updated_at_ms", NpgsqlDbType.Bigint, snapshot.CapturedAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<long> LockSnapshotStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-snapshot-lock-stream",
            static schema => $"""
                SELECT "current_version"
                FROM {schema.RelationshipProjectionVersionsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                FOR UPDATE;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return value is long version
            ? version
            : throw new InvalidOperationException("Relationship projection stream was not initialized.");
    }

    private async Task<(long OwnerUserId, RelationshipProjectionListType ListType, long Version, string ResourceHash)?>
        TryReadSnapshotAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            string snapshotId,
            CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-read-snapshot",
            static schema => $"""
                SELECT "owner_user_id", "list_type", "version", "resource_hash"
                FROM {schema.RelationshipProjectionSnapshotsTableSql}
                WHERE "snapshot_id" = @snapshot_id;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("snapshot_id", NpgsqlDbType.Varchar, snapshotId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return (
            reader.GetInt64(0),
            (RelationshipProjectionListType)reader.GetInt16(1),
            reader.GetInt64(2),
            reader.GetString(3));
    }

    private async Task DeleteSnapshotStreamItemsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-delete-snapshot-stream-items",
            static schema => $"""
                DELETE FROM {schema.RelationshipProjectionItemsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task InsertSnapshotItemsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-insert-snapshot-items",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionItemsTableSql}
                    ("owner_user_id", "list_type", "resource_id", "subject_user_id",
                     "actor_user_id", "state", "message", "version", "occurred_at_ms")
                SELECT @owner_user_id, @list_type, value.resource_id, value.subject_user_id,
                       value.actor_user_id, value.state, value.message, @version,
                       value.occurred_at_ms
                FROM unnest(
                    @resource_ids::text[],
                    @subject_user_ids::bigint[],
                    @actor_user_ids::bigint[],
                    @states::text[],
                    @messages::text[],
                    @occurred_at_values::bigint[])
                AS value(resource_id, subject_user_id, actor_user_id, state, message, occurred_at_ms);
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, snapshot.Version);
        command.Parameters.AddWithValue(
            "resource_ids", NpgsqlDbType.Array | NpgsqlDbType.Text,
            snapshot.Items.Select(static item => item.ResourceId).ToArray());
        command.Parameters.AddWithValue(
            "subject_user_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            snapshot.Items.Select(static item => item.SubjectUserId).ToArray());
        command.Parameters.AddWithValue(
            "actor_user_ids", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            snapshot.Items.Select(static item => item.ActorUserId).ToArray());
        command.Parameters.AddWithValue(
            "states", NpgsqlDbType.Array | NpgsqlDbType.Text,
            snapshot.Items.Select(static item => item.State).ToArray());
        command.Parameters.AddWithValue(
            "messages", NpgsqlDbType.Array | NpgsqlDbType.Text,
            snapshot.Items.Select(static item => item.Message).ToArray());
        command.Parameters.AddWithValue(
            "occurred_at_values", NpgsqlDbType.Array | NpgsqlDbType.Bigint,
            snapshot.Items.Select(static item => item.OccurredAtMs).ToArray());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task InsertSnapshotCheckpointAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-insert-snapshot-checkpoint",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionSnapshotsTableSql}
                    ("snapshot_id", "owner_user_id", "list_type", "version", "item_count",
                     "resource_hash", "captured_at_ms", "applied_at_ms")
                VALUES
                    (@snapshot_id, @owner_user_id, @list_type, @version, @item_count,
                     @resource_hash, @captured_at_ms, @applied_at_ms);
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        command.Parameters.AddWithValue("snapshot_id", NpgsqlDbType.Varchar, snapshot.SnapshotId);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, snapshot.Version);
        command.Parameters.AddWithValue("item_count", NpgsqlDbType.Integer, snapshot.ItemCount);
        command.Parameters.AddWithValue("resource_hash", NpgsqlDbType.Varchar, snapshot.ResourceHash);
        command.Parameters.AddWithValue("captured_at_ms", NpgsqlDbType.Bigint, snapshot.CapturedAtMs);
        command.Parameters.AddWithValue(
            "applied_at_ms", NpgsqlDbType.Bigint, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task AdvanceSnapshotStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionStreamSnapshot snapshot,
        long currentVersion,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-advance-snapshot-stream",
            static schema => $"""
                UPDATE {schema.RelationshipProjectionVersionsTableSql}
                SET "current_version" = @snapshot_version,
                    "updated_at_ms" = GREATEST("updated_at_ms", @updated_at_ms)
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                  AND "current_version" = @current_version;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddSnapshotStreamParameters(command, snapshot);
        command.Parameters.AddWithValue("snapshot_version", NpgsqlDbType.Bigint, snapshot.Version);
        command.Parameters.AddWithValue("current_version", NpgsqlDbType.Bigint, currentVersion);
        command.Parameters.AddWithValue("updated_at_ms", NpgsqlDbType.Bigint, snapshot.CapturedAtMs);
        var updated = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
        if (updated != 1)
            throw new InvalidOperationException("Relationship projection snapshot stream was not retained.");
    }

    private async Task<bool> IsCoveredBySnapshotAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-is-covered-by-snapshot",
            static schema => $"""
                SELECT EXISTS (
                    SELECT 1
                    FROM {schema.RelationshipProjectionSnapshotsTableSql}
                    WHERE "owner_user_id" = @owner_user_id
                      AND "list_type" = @list_type
                      AND "version" >= @version);
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, delta.Version);
        return await command.ExecuteScalarAsync(ct).ConfigureAwait(false) is true;
    }

    private static void AddSnapshotStreamParameters(
        NpgsqlCommand command,
        RelationshipProjectionStreamSnapshot snapshot)
    {
        command.Parameters.AddWithValue("owner_user_id", NpgsqlDbType.Bigint, snapshot.OwnerUserId);
        command.Parameters.AddWithValue("list_type", NpgsqlDbType.Smallint, (short)snapshot.ListType);
    }

    private async Task EnsureStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-ensure-stream",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionVersionsTableSql}
                    ("owner_user_id", "list_type", "current_version", "updated_at_ms")
                VALUES (@owner_user_id, @list_type, 0, @updated_at_ms)
                ON CONFLICT ("owner_user_id", "list_type") DO NOTHING;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("updated_at_ms", NpgsqlDbType.Bigint, delta.OccurredAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task<long> LockStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-lock-stream",
            static schema => $"""
                SELECT "current_version"
                FROM {schema.RelationshipProjectionVersionsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                FOR UPDATE;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        var value = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return value is long version
            ? version
            : throw new InvalidOperationException("Relationship projection stream was not initialized.");
    }

    private async Task<(long OwnerUserId, RelationshipProjectionListType ListType, long Version)?>
        TryReadInboxAsync(
            NpgsqlConnection connection,
            NpgsqlTransaction transaction,
            string eventId,
            CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-read-inbox",
            static schema => $"""
                SELECT "owner_user_id", "list_type", "version"
                FROM {schema.RelationshipProjectionInboxTableSql}
                WHERE "event_id" = @event_id;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Varchar, eventId);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return (
            reader.GetInt64(0),
            (RelationshipProjectionListType)reader.GetInt16(1),
            reader.GetInt64(2));
    }

    private async Task UpsertItemAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-upsert-item",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionItemsTableSql}
                    ("owner_user_id", "list_type", "resource_id", "subject_user_id",
                     "actor_user_id", "state", "message", "version", "occurred_at_ms")
                VALUES
                    (@owner_user_id, @list_type, @resource_id, @subject_user_id,
                     @actor_user_id, @state, @message, @version, @occurred_at_ms)
                ON CONFLICT ("owner_user_id", "list_type", "resource_id") DO UPDATE
                SET "subject_user_id" = EXCLUDED."subject_user_id",
                    "actor_user_id" = EXCLUDED."actor_user_id",
                    "state" = EXCLUDED."state",
                    "message" = EXCLUDED."message",
                    "version" = EXCLUDED."version",
                    "occurred_at_ms" = EXCLUDED."occurred_at_ms";
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("resource_id", NpgsqlDbType.Varchar, delta.ResourceId);
        command.Parameters.AddWithValue("subject_user_id", NpgsqlDbType.Bigint, delta.SubjectUserId);
        command.Parameters.AddWithValue("actor_user_id", NpgsqlDbType.Bigint, delta.ActorUserId);
        command.Parameters.AddWithValue("state", NpgsqlDbType.Varchar, (object?)delta.State ?? DBNull.Value);
        command.Parameters.AddWithValue("message", NpgsqlDbType.Varchar, (object?)delta.Message ?? DBNull.Value);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, delta.Version);
        command.Parameters.AddWithValue("occurred_at_ms", NpgsqlDbType.Bigint, delta.OccurredAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task DeleteItemAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-delete-item",
            static schema => $"""
                DELETE FROM {schema.RelationshipProjectionItemsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                  AND "resource_id" = @resource_id;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("resource_id", NpgsqlDbType.Varchar, delta.ResourceId);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task InsertInboxAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-insert-inbox",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionInboxTableSql}
                    ("event_id", "owner_user_id", "list_type", "version", "processed_at_ms")
                VALUES (@event_id, @owner_user_id, @list_type, @version, @processed_at_ms);
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Varchar, delta.EventId);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, delta.Version);
        command.Parameters.AddWithValue(
            "processed_at_ms",
            NpgsqlDbType.Bigint,
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private async Task InsertHistoryAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-insert-history",
            static schema => $"""
                INSERT INTO {schema.RelationshipProjectionHistoryTableSql}
                    ("owner_user_id", "list_type", "version", "event_id", "operation",
                     "resource_id", "subject_user_id", "actor_user_id", "state",
                     "message", "occurred_at_ms")
                VALUES
                    (@owner_user_id, @list_type, @version, @event_id, @operation,
                     @resource_id, @subject_user_id, @actor_user_id, @state,
                     @message, @occurred_at_ms);
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("version", NpgsqlDbType.Bigint, delta.Version);
        command.Parameters.AddWithValue("event_id", NpgsqlDbType.Varchar, delta.EventId);
        command.Parameters.AddWithValue("operation", NpgsqlDbType.Smallint, (short)delta.Operation);
        command.Parameters.AddWithValue("resource_id", NpgsqlDbType.Varchar, delta.ResourceId);
        command.Parameters.AddWithValue("subject_user_id", NpgsqlDbType.Bigint, delta.SubjectUserId);
        command.Parameters.AddWithValue("actor_user_id", NpgsqlDbType.Bigint, delta.ActorUserId);
        command.Parameters.AddWithValue("state", NpgsqlDbType.Varchar, (object?)delta.State ?? DBNull.Value);
        command.Parameters.AddWithValue("message", NpgsqlDbType.Varchar, (object?)delta.Message ?? DBNull.Value);
        command.Parameters.AddWithValue("occurred_at_ms", NpgsqlDbType.Bigint, delta.OccurredAtMs);
        await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    public async Task<IReadOnlyList<RelationshipProjectionHistoryEntry>> QueryHistoryAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long fromVersionExclusive,
        int limit,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ownerUserId);
        if (!Enum.IsDefined(listType))
            throw new ArgumentOutOfRangeException(nameof(listType));
        if (fromVersionExclusive < 0)
            throw new ArgumentOutOfRangeException(nameof(fromVersionExclusive));
        if (limit is < 1 or > 200)
            throw new ArgumentOutOfRangeException(nameof(limit));

        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-query-history",
            static schema => $"""
                SELECT "version", "event_id", "operation", "resource_id", "subject_user_id",
                       "actor_user_id", "state", "message", "occurred_at_ms"
                FROM {schema.RelationshipProjectionHistoryTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                  AND "version" > @from_version
                ORDER BY "version"
                LIMIT @limit;
                """);
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("owner_user_id", NpgsqlDbType.Bigint, ownerUserId);
        command.Parameters.AddWithValue("list_type", NpgsqlDbType.Smallint, (short)listType);
        command.Parameters.AddWithValue("from_version", NpgsqlDbType.Bigint, fromVersionExclusive);
        command.Parameters.AddWithValue("limit", NpgsqlDbType.Integer, limit);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var entries = new List<RelationshipProjectionHistoryEntry>(limit);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            entries.Add(new RelationshipProjectionHistoryEntry
            {
                OwnerUserId = ownerUserId,
                ListType = listType,
                Version = reader.GetInt64(0),
                EventId = reader.GetString(1),
                Operation = (RelationshipProjectionOperation)reader.GetInt16(2),
                ResourceId = reader.GetString(3),
                SubjectUserId = reader.GetInt64(4),
                ActorUserId = reader.GetInt64(5),
                State = reader.IsDBNull(6) ? null : reader.GetString(6),
                Message = reader.IsDBNull(7) ? null : reader.GetString(7),
                OccurredAtMs = reader.GetInt64(8)
            });
        }

        return entries;
    }

    public async Task<long> GetRetentionFloorAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ownerUserId);
        if (!Enum.IsDefined(listType))
            throw new ArgumentOutOfRangeException(nameof(listType));

        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-retention-floor",
            static schema => $"""
                SELECT COALESCE(MIN("version"), 0)
                FROM {schema.RelationshipProjectionHistoryTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type;
                """);
        await using var connection = await _databaseClient
            .GetDataSource().OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var command = new NpgsqlCommand(sql, connection);
        command.Parameters.AddWithValue("owner_user_id", NpgsqlDbType.Bigint, ownerUserId);
        command.Parameters.AddWithValue("list_type", NpgsqlDbType.Smallint, (short)listType);
        var result = await command.ExecuteScalarAsync(ct).ConfigureAwait(false);
        return result is long value ? value : 0L;
    }

    private async Task<int> AdvanceStreamAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        RelationshipProjectionDelta delta,
        long currentVersion,
        CancellationToken ct)
    {
        var sql = _schema.GetOrAddCommandText(
            "relationship-projection-advance-stream",
            static schema => $"""
                UPDATE {schema.RelationshipProjectionVersionsTableSql}
                SET "current_version" = @next_version,
                    "updated_at_ms" = @updated_at_ms
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                  AND "current_version" = @current_version;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, delta);
        command.Parameters.AddWithValue("next_version", NpgsqlDbType.Bigint, delta.Version);
        command.Parameters.AddWithValue("updated_at_ms", NpgsqlDbType.Bigint, delta.OccurredAtMs);
        command.Parameters.AddWithValue("current_version", NpgsqlDbType.Bigint, currentVersion);
        return await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);
    }

    private static void AddStreamParameters(
        NpgsqlCommand command,
        RelationshipProjectionDelta delta)
    {
        command.Parameters.AddWithValue("owner_user_id", NpgsqlDbType.Bigint, delta.OwnerUserId);
        command.Parameters.AddWithValue("list_type", NpgsqlDbType.Smallint, (short)delta.ListType);
    }

    private static void Validate(RelationshipProjectionDelta delta)
    {
        ArgumentNullException.ThrowIfNull(delta);
        if (delta.SchemaVersion != RelationshipProjectionDelta.CurrentSchemaVersion)
            throw new NotSupportedException(
                $"Unsupported relationship projection schema version: {delta.SchemaVersion}.");
        if (string.IsNullOrWhiteSpace(delta.EventId) || delta.EventId.Length > 64)
            throw new ArgumentException("Projection event id is invalid.", nameof(delta));
        if (delta.OwnerUserId <= 0 || delta.SubjectUserId <= 0 || delta.ActorUserId <= 0)
            throw new ArgumentException("Projection user ids must be positive.", nameof(delta));
        if (!Enum.IsDefined(delta.ListType) || !Enum.IsDefined(delta.Operation))
            throw new ArgumentException("Projection enum value is unknown.", nameof(delta));
        if (delta.Version <= 0)
            throw new ArgumentException("Projection version must be positive.", nameof(delta));
        if (string.IsNullOrWhiteSpace(delta.ResourceId) || delta.ResourceId.Length > 128)
            throw new ArgumentException("Projection resource id is invalid.", nameof(delta));
        if (delta.State is { Length: > 32 } || delta.Message is { Length: > 512 })
            throw new ArgumentException("Projection state or message exceeds its limit.", nameof(delta));
        if (delta.OccurredAtMs <= 0)
            throw new ArgumentException("Projection occurrence time must be positive.", nameof(delta));
    }

    private static void ValidateSnapshot(RelationshipProjectionStreamSnapshot snapshot)
    {
        ArgumentNullException.ThrowIfNull(snapshot);
        if (snapshot.SchemaVersion != RelationshipProjectionStreamSnapshot.CurrentSchemaVersion)
            throw new NotSupportedException(
                $"Unsupported relationship snapshot schema version: {snapshot.SchemaVersion}.");
        if (snapshot.OwnerUserId <= 0 || !Enum.IsDefined(snapshot.ListType))
            throw new ArgumentException("Snapshot stream identity is invalid.", nameof(snapshot));
        if (snapshot.Version < 0 || snapshot.CapturedAtMs <= 0)
            throw new ArgumentException("Snapshot version or capture time is invalid.", nameof(snapshot));
        if (snapshot.Items.Count > MaxSnapshotItems || snapshot.ItemCount != snapshot.Items.Count)
            throw new ArgumentException("Snapshot item count is invalid.", nameof(snapshot));

        string? previous = null;
        foreach (var item in snapshot.Items)
        {
            if (string.IsNullOrWhiteSpace(item.ResourceId) || item.ResourceId.Length > 128
                || item.SubjectUserId <= 0 || item.ActorUserId <= 0 || item.OccurredAtMs <= 0
                || item.State is { Length: > 32 } || item.Message is { Length: > 512 })
            {
                throw new ArgumentException("Snapshot item is invalid.", nameof(snapshot));
            }
            if (previous is not null
                && string.CompareOrdinal(previous, item.ResourceId) >= 0)
            {
                throw new ArgumentException(
                    "Snapshot resource ids must be strictly ordered and unique.", nameof(snapshot));
            }
            previous = item.ResourceId;
        }

        var resourceHash = RelationshipProjectionSnapshotHash.Compute(
            snapshot.Items.Select(static item => item.ResourceId));
        if (!string.Equals(resourceHash, snapshot.ResourceHash, StringComparison.Ordinal))
            throw new ArgumentException("Snapshot resource hash does not match its items.", nameof(snapshot));

        var snapshotId = RelationshipEventIdFactory.CreateRelationshipProjectionSnapshotId(
            snapshot.OwnerUserId,
            snapshot.ListType,
            snapshot.Version,
            snapshot.ResourceHash);
        if (!string.Equals(snapshotId, snapshot.SnapshotId, StringComparison.Ordinal))
            throw new ArgumentException("Snapshot id does not match its stream and hash.", nameof(snapshot));
    }
}

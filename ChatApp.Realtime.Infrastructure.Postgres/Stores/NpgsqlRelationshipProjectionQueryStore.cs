using System.Data;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Npgsql;
using NpgsqlTypes;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRelationshipProjectionQueryStore(
    RealtimeDatabaseClient databaseClient,
    RealtimeDatabaseSchema schema) : IRelationshipProjectionQueryStore
{
    public async Task<RelationshipProjectionReadPage> ReadAsync(
        long ownerUserId,
        RelationshipProjectionListType listType,
        int pageSize,
        long? expectedVersion,
        string? afterResourceId,
        CancellationToken ct = default)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(ownerUserId);
        if (!Enum.IsDefined(listType))
            throw new ArgumentOutOfRangeException(nameof(listType));
        if (pageSize is < 1 or > 200)
            throw new ArgumentOutOfRangeException(nameof(pageSize));
        if (expectedVersion is < 0)
            throw new ArgumentOutOfRangeException(nameof(expectedVersion));
        if (afterResourceId is { Length: > 128 })
            throw new ArgumentOutOfRangeException(nameof(afterResourceId));
        if (expectedVersion is null != (afterResourceId is null))
            throw new ArgumentException("Both projection cursor components must be supplied together.");

        await using var connection = await databaseClient.GetDataSource()
            .OpenConnectionAsync(ct).ConfigureAwait(false);
        await using var transaction = await connection
            .BeginTransactionAsync(IsolationLevel.RepeatableRead, ct)
            .ConfigureAwait(false);
        var readiness = await ReadReadinessAsync(
                connection,
                transaction,
                ownerUserId,
                listType,
                ct)
            .ConfigureAwait(false);
        if (readiness is null || !readiness.Value.HasSnapshotBaseline)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new RelationshipProjectionReadPage(
                RelationshipProjectionReadStatus.Unavailable,
                readiness?.CurrentVersion ?? 0,
                [],
                false,
                null);
        }

        if (expectedVersion is { } cursorVersion
            && cursorVersion != readiness.Value.CurrentVersion)
        {
            await transaction.CommitAsync(ct).ConfigureAwait(false);
            return new RelationshipProjectionReadPage(
                RelationshipProjectionReadStatus.VersionChanged,
                readiness.Value.CurrentVersion,
                [],
                false,
                null);
        }

        var items = await ReadItemsAsync(
                connection,
                transaction,
                ownerUserId,
                listType,
                pageSize,
                afterResourceId,
                ct)
            .ConfigureAwait(false);
        await transaction.CommitAsync(ct).ConfigureAwait(false);

        var hasMore = items.Count > pageSize;
        if (hasMore)
            items.RemoveAt(items.Count - 1);
        return new RelationshipProjectionReadPage(
            RelationshipProjectionReadStatus.Ready,
            readiness.Value.CurrentVersion,
            items,
            hasMore,
            hasMore ? items[^1].ResourceId : null);
    }

    private async Task<(long CurrentVersion, bool HasSnapshotBaseline)?> ReadReadinessAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long ownerUserId,
        RelationshipProjectionListType listType,
        CancellationToken ct)
    {
        var sql = schema.GetOrAddCommandText(
            "relationship-projection-read-readiness",
            static current => $"""
                SELECT version."current_version",
                       EXISTS (
                           SELECT 1
                           FROM {current.RelationshipProjectionSnapshotsTableSql} AS snapshot
                           WHERE snapshot."owner_user_id" = version."owner_user_id"
                             AND snapshot."list_type" = version."list_type"
                             AND snapshot."version" <= version."current_version")
                FROM {current.RelationshipProjectionVersionsTableSql} AS version
                WHERE version."owner_user_id" = @owner_user_id
                  AND version."list_type" = @list_type;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, ownerUserId, listType);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        if (!await reader.ReadAsync(ct).ConfigureAwait(false))
            return null;
        return (reader.GetInt64(0), reader.GetBoolean(1));
    }

    private async Task<List<RelationshipListItem>> ReadItemsAsync(
        NpgsqlConnection connection,
        NpgsqlTransaction transaction,
        long ownerUserId,
        RelationshipProjectionListType listType,
        int pageSize,
        string? afterResourceId,
        CancellationToken ct)
    {
        var sql = schema.GetOrAddCommandText(
            "relationship-projection-read-page",
            static current => $"""
                SELECT "resource_id", "subject_user_id", "state", "message", "occurred_at_ms"
                FROM {current.RelationshipProjectionItemsTableSql}
                WHERE "owner_user_id" = @owner_user_id
                  AND "list_type" = @list_type
                  AND (@after_resource_id IS NULL OR "resource_id" > @after_resource_id)
                ORDER BY "resource_id"
                LIMIT @limit;
                """);
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        AddStreamParameters(command, ownerUserId, listType);
        command.Parameters.AddWithValue(
            "after_resource_id",
            NpgsqlDbType.Varchar,
            (object?)afterResourceId ?? DBNull.Value);
        command.Parameters.AddWithValue("limit", NpgsqlDbType.Integer, pageSize + 1);
        await using var reader = await command.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var items = new List<RelationshipListItem>(pageSize + 1);
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
        {
            items.Add(new RelationshipListItem
            {
                ResourceId = reader.GetString(0),
                UserId = reader.GetInt64(1),
                Status = reader.IsDBNull(2) ? null : reader.GetString(2),
                Message = reader.IsDBNull(3) ? null : reader.GetString(3),
                CreatedAtMs = reader.GetInt64(4)
            });
        }

        return items;
    }

    private static void AddStreamParameters(
        NpgsqlCommand command,
        long ownerUserId,
        RelationshipProjectionListType listType)
    {
        command.Parameters.AddWithValue("owner_user_id", NpgsqlDbType.Bigint, ownerUserId);
        command.Parameters.AddWithValue("list_type", NpgsqlDbType.Smallint, (short)listType);
    }
}

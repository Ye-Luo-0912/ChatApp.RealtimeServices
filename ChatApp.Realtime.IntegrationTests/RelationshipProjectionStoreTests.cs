using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;

namespace ChatApp.Realtime.IntegrationTests;

[Collection(nameof(RealtimePipelineCollection))]
public sealed class RelationshipProjectionStoreTests
{
    private readonly RealtimePipelineFixture _fixture;

    public RelationshipProjectionStoreTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task Apply_Duplicate_Delete_AdvanceOneContiguousStream()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_001;
        var first = CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            RelationshipProjectionOperation.Upsert);

        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(first));
        Assert.Equal(RelationshipProjectionApplyResult.Duplicate, await store.ApplyAsync(first));

        var second = CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            2,
            RelationshipProjectionOperation.Delete);
        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(second));

        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT "current_version"
                  FROM {schema.RelationshipProjectionVersionsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*)
                  FROM {schema.RelationshipProjectionItemsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*)
                  FROM {schema.RelationshipProjectionInboxTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type);
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("list_type", (short)RelationshipProjectionListType.Friends);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2, reader.GetInt64(0));
        Assert.Equal(0, reader.GetInt64(1));
        Assert.Equal(2, reader.GetInt64(2));
    }

    [Fact]
    public async Task Gap_RollsBackProjectionInboxAndVersion()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_002;
        var gap = CreateDelta(
            owner,
            RelationshipProjectionListType.BlockedUsers,
            2,
            RelationshipProjectionOperation.Upsert);

        var error = await Assert.ThrowsAsync<RelationshipProjectionGapException>(
            () => store.ApplyAsync(gap));
        Assert.Equal(1, error.ExpectedVersion);
        Assert.Equal(2, error.ActualVersion);

        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionVersionsTableSql}
                  WHERE "owner_user_id" = @owner),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionItemsTableSql}
                  WHERE "owner_user_id" = @owner),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionInboxTableSql}
                  WHERE "owner_user_id" = @owner);
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(0, reader.GetInt64(0));
        Assert.Equal(0, reader.GetInt64(1));
        Assert.Equal(0, reader.GetInt64(2));
    }

    [Fact]
    public async Task ConcurrentDuplicate_IsAppliedExactlyOnce()
    {
        await using var client = CreateClient();
        var store = new NpgsqlRelationshipProjectionStore(
            client, new RealtimeDatabaseSchema("realtime"));
        var delta = CreateDelta(
            9_100_003,
            RelationshipProjectionListType.FriendRequests,
            1,
            RelationshipProjectionOperation.Upsert);

        var results = await Task.WhenAll(store.ApplyAsync(delta), store.ApplyAsync(delta));

        Assert.Contains(RelationshipProjectionApplyResult.Applied, results);
        Assert.Contains(RelationshipProjectionApplyResult.Duplicate, results);
    }

    [Fact]
    public async Task SnapshotBaseline_AdvancesEmptyStream_IsIdempotent_AndNextDeltaAdvances()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_004;
        var snapshot = CreateSnapshot(owner, RelationshipProjectionListType.Friends, 5, "friend-1");

        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Applied,
            await store.ApplySnapshotAsync(snapshot));
        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Duplicate,
            await store.ApplySnapshotAsync(snapshot));

        var next = CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            6,
            RelationshipProjectionOperation.Upsert,
            resourceId: "friend-2",
            subjectUserId: owner + 2);
        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(next));

        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT "current_version" FROM {schema.RelationshipProjectionVersionsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionItemsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionSnapshotsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type);
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("list_type", (short)RelationshipProjectionListType.Friends);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(6, reader.GetInt64(0));
        Assert.Equal(2, reader.GetInt64(1));
        Assert.Equal(1, reader.GetInt64(2));
    }

    [Fact]
    public async Task DuplicateSnapshot_ReconcilesCountAndResourceHashBeforeReportingVerified()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_007;
        var snapshot = CreateSnapshot(
            owner,
            RelationshipProjectionListType.Friends,
            4,
            "friend-repair");
        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Applied,
            await store.ApplySnapshotAsync(snapshot));

        await using (var connection = new NpgsqlConnection(_fixture.PostgresConnectionString))
        {
            await connection.OpenAsync();
            await using var corrupt = new NpgsqlCommand(
                $"""
                 DELETE FROM {schema.RelationshipProjectionItemsTableSql}
                 WHERE "owner_user_id" = @owner AND "list_type" = @list_type;
                 """,
                connection);
            corrupt.Parameters.AddWithValue("owner", owner);
            corrupt.Parameters.AddWithValue(
                "list_type",
                (short)RelationshipProjectionListType.Friends);
            Assert.Equal(1, await corrupt.ExecuteNonQueryAsync());
        }

        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Applied,
            await store.ApplySnapshotAsync(snapshot));
        Assert.Equal(
            RelationshipProjectionSnapshotApplyResult.Duplicate,
            await store.ApplySnapshotAsync(snapshot));

        await using var verifyConnection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await verifyConnection.OpenAsync();
        await using var verify = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionItemsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionSnapshotsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type);
             """,
            verifyConnection);
        verify.Parameters.AddWithValue("owner", owner);
        verify.Parameters.AddWithValue(
            "list_type",
            (short)RelationshipProjectionListType.Friends);
        await using var reader = await verify.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(1, reader.GetInt64(0));
        Assert.Equal(1, reader.GetInt64(1));
    }

    [Fact]
    public async Task DeltaCoveredBySnapshot_IsAcknowledgedAsDuplicateWithoutReplayingOldState()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_006;
        var snapshot = CreateSnapshot(
            owner,
            RelationshipProjectionListType.FriendRequests,
            5,
            "snapshot-request");
        await store.ApplySnapshotAsync(snapshot);

        var covered = CreateDelta(
            owner,
            RelationshipProjectionListType.FriendRequests,
            3,
            RelationshipProjectionOperation.Upsert,
            resourceId: "obsolete-request");

        Assert.Equal(
            RelationshipProjectionApplyResult.Duplicate,
            await store.ApplyAsync(covered));

        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT "current_version" FROM {schema.RelationshipProjectionVersionsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT string_agg("resource_id", ',' ORDER BY "resource_id")
                  FROM {schema.RelationshipProjectionItemsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionInboxTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type);
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue(
            "list_type",
            (short)RelationshipProjectionListType.FriendRequests);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(5, reader.GetInt64(0));
        Assert.Equal("snapshot-request", reader.GetString(1));
        Assert.Equal(0, reader.GetInt64(2));
    }

    [Fact]
    public async Task SnapshotVersionMismatch_RollsBackWithoutReplacingNewerProjection()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_005;
        var first = CreateDelta(
            owner,
            RelationshipProjectionListType.BlockedUsers,
            1,
            RelationshipProjectionOperation.Upsert);
        await store.ApplyAsync(first);

        var stale = CreateSnapshot(
            owner,
            RelationshipProjectionListType.BlockedUsers,
            0,
            "stale-resource");
        var error = await Assert.ThrowsAsync<RelationshipProjectionSnapshotVersionMismatchException>(
            () => store.ApplySnapshotAsync(stale));
        Assert.Equal(0, error.SnapshotVersion);
        Assert.Equal(1, error.CurrentVersion);

        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT "resource_id"
             FROM {schema.RelationshipProjectionItemsTableSql}
             WHERE "owner_user_id" = @owner AND "list_type" = @list_type;
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("list_type", (short)RelationshipProjectionListType.BlockedUsers);
        Assert.Equal(first.ResourceId, (string?)await command.ExecuteScalarAsync());
    }

    [Fact]
    public async Task Query_RequiresSnapshotBaseline_AndRejectsStaleContinuationVersion()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var projectionStore = new NpgsqlRelationshipProjectionStore(client, schema);
        var queryStore = new NpgsqlRelationshipProjectionQueryStore(client, schema);
        const long owner = 9_100_008;

        await projectionStore.ApplyAsync(CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            RelationshipProjectionOperation.Upsert,
            resourceId: "delta-only"));

        var unavailable = await queryStore.ReadAsync(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            null,
            null);
        Assert.Equal(RelationshipProjectionReadStatus.Unavailable, unavailable.Status);
        Assert.Equal(1, unavailable.CurrentVersion);

        await projectionStore.ApplySnapshotAsync(CreateSnapshot(
            owner,
            RelationshipProjectionListType.Friends,
            5,
            "friend-b",
            "friend-a"));

        var firstPage = await queryStore.ReadAsync(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            null,
            null);
        Assert.Equal(RelationshipProjectionReadStatus.Ready, firstPage.Status);
        Assert.Equal(5, firstPage.CurrentVersion);
        Assert.True(firstPage.HasMore);
        Assert.Equal("friend-a", Assert.Single(firstPage.Items).ResourceId);
        Assert.Equal("friend-a", firstPage.NextResourceId);

        await projectionStore.ApplyAsync(CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            6,
            RelationshipProjectionOperation.Upsert,
            resourceId: "friend-c",
            subjectUserId: owner + 3));

        var staleContinuation = await queryStore.ReadAsync(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            firstPage.CurrentVersion,
            firstPage.NextResourceId);
        Assert.Equal(RelationshipProjectionReadStatus.VersionChanged, staleContinuation.Status);
        Assert.Equal(6, staleContinuation.CurrentVersion);
        Assert.Empty(staleContinuation.Items);

        var refreshed = await queryStore.ReadAsync(
            owner,
            RelationshipProjectionListType.Friends,
            10,
            null,
            null);
        Assert.Equal(RelationshipProjectionReadStatus.Ready, refreshed.Status);
        Assert.Equal(
            ["friend-a", "friend-b", "friend-c"],
            refreshed.Items.Select(static item => item.ResourceId));
    }

    [Fact]
    public async Task Apply_WritesVersionedHistoryAtomically_AndCatchUpReadsStrictlyAfter()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_011;

        var first = CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            1,
            RelationshipProjectionOperation.Upsert,
            resourceId: "friend-1");
        var second = CreateDelta(
            owner,
            RelationshipProjectionListType.Friends,
            2,
            RelationshipProjectionOperation.Upsert,
            resourceId: "friend-2");
        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(first));
        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(second));

        // Catch-up from the first version returns the delta that is strictly after it.
        var catchUp = await store.QueryHistoryAsync(
            owner,
            RelationshipProjectionListType.Friends,
            fromVersionExclusive: 1,
            limit: 10);
        var entry = Assert.Single(catchUp);
        Assert.Equal(2, entry.Version);
        Assert.Equal("friend-2", entry.ResourceId);
        Assert.Equal(RelationshipProjectionOperation.Upsert, entry.Operation);
        Assert.Equal(second.EventId, entry.EventId);
        Assert.Equal(second.SubjectUserId, entry.SubjectUserId);

        // Catch-up from the current version is empty.
        Assert.Empty(await store.QueryHistoryAsync(
            owner,
            RelationshipProjectionListType.Friends,
            fromVersionExclusive: 2,
            limit: 10));

        // History count matches applied deltas (atomic with item/inbox/version).
        await using var connection = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             SELECT
                 (SELECT COUNT(*) FROM {schema.RelationshipProjectionHistoryTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type),
                 (SELECT "current_version" FROM {schema.RelationshipProjectionVersionsTableSql}
                  WHERE "owner_user_id" = @owner AND "list_type" = @list_type);
             """,
            connection);
        command.Parameters.AddWithValue("owner", owner);
        command.Parameters.AddWithValue("list_type", (short)RelationshipProjectionListType.Friends);
        await using var reader = await command.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.Equal(2, reader.GetInt64(0));
        Assert.Equal(2, reader.GetInt64(1));
    }

    [Fact]
    public async Task DuplicateApply_DoesNotRepeatHistoryEntry()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_012;
        var delta = CreateDelta(
            owner,
            RelationshipProjectionListType.BlockedUsers,
            1,
            RelationshipProjectionOperation.Upsert);

        Assert.Equal(RelationshipProjectionApplyResult.Applied, await store.ApplyAsync(delta));
        Assert.Equal(RelationshipProjectionApplyResult.Duplicate, await store.ApplyAsync(delta));

        Assert.Single(await store.QueryHistoryAsync(
            owner,
            RelationshipProjectionListType.BlockedUsers,
            fromVersionExclusive: 0,
            limit: 10));
    }

    [Fact]
    public async Task Gap_RollsBackHistoryTogetherWithItemAndInbox()
    {
        await using var client = CreateClient();
        var schema = new RealtimeDatabaseSchema("realtime");
        var store = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_100_013;
        var gap = CreateDelta(
            owner,
            RelationshipProjectionListType.FriendRequests,
            2,
            RelationshipProjectionOperation.Upsert);

        await Assert.ThrowsAsync<RelationshipProjectionGapException>(() => store.ApplyAsync(gap));

        Assert.Empty(await store.QueryHistoryAsync(
            owner,
            RelationshipProjectionListType.FriendRequests,
            fromVersionExclusive: 0,
            limit: 10));
    }

    private RealtimeDatabaseClient CreateClient() => new(
        _fixture.PostgresConnectionString,
        NullLogger<RealtimeDatabaseClient>.Instance);

    private static RelationshipProjectionDelta CreateDelta(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long version,
        RelationshipProjectionOperation operation,
        string resourceId = "resource-1",
        long? subjectUserId = null)
    {
        return new RelationshipProjectionDelta
        {
            EventId = RelationshipEventIdFactory.CreateRelationshipProjectionEventId(
                ownerUserId, listType, version),
            OwnerUserId = ownerUserId,
            ListType = listType,
            Version = version,
            Operation = operation,
            ResourceId = resourceId,
            SubjectUserId = subjectUserId ?? ownerUserId + 1,
            ActorUserId = ownerUserId,
            State = operation == RelationshipProjectionOperation.Upsert ? "Pending" : "Delete",
            Message = operation == RelationshipProjectionOperation.Upsert ? "hello" : null,
            OccurredAtMs = 1_800_000_000_000 + version
        };
    }

    private static RelationshipProjectionStreamSnapshot CreateSnapshot(
        long ownerUserId,
        RelationshipProjectionListType listType,
        long version,
        params string[] resourceIds)
    {
        var items = resourceIds
            .Order(StringComparer.Ordinal)
            .Select((resourceId, index) => new RelationshipProjectionSnapshotItem
            {
                ResourceId = resourceId,
                SubjectUserId = ownerUserId + index + 1,
                ActorUserId = ownerUserId,
                State = listType == RelationshipProjectionListType.BlockedUsers
                    ? "Blocked"
                    : "Accepted",
                OccurredAtMs = 1_800_000_000_000
            })
            .ToArray();
        var resourceHash = RelationshipProjectionSnapshotHash.Compute(
            items.Select(static item => item.ResourceId));
        return new RelationshipProjectionStreamSnapshot
        {
            SnapshotId = RelationshipEventIdFactory.CreateRelationshipProjectionSnapshotId(
                ownerUserId, listType, version, resourceHash),
            OwnerUserId = ownerUserId,
            ListType = listType,
            Version = version,
            CapturedAtMs = 1_800_000_000_001,
            ItemCount = items.Length,
            ResourceHash = resourceHash,
            Items = items
        };
    }
}

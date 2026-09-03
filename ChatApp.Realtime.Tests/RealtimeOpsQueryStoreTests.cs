using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Relationships;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class RealtimeOpsQueryStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer? _postgres = string.IsNullOrEmpty(ExternalPostgresConnectionString()) ? new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build() : null;
    private readonly string _externalPostgres = ExternalPostgresConnectionString() ?? string.Empty;
    private readonly string _schemaSuffix = Guid.NewGuid().ToString("N")[..8];

    private static string? ExternalPostgresConnectionString() => Environment.GetEnvironmentVariable("CHATAPP_TEST_POSTGRES");

    private string PostgresConnectionString => _postgres?.GetConnectionString() ?? _externalPostgres;

    public Task InitializeAsync() => _postgres?.StartAsync() ?? Task.CompletedTask;

    public Task DisposeAsync() => _postgres?.DisposeAsync().AsTask() ?? Task.CompletedTask;

    [Fact]
    public async Task MigrationProgress_ReportsOpenCheckpoint_When009Deferred()
    {
        const string schemaName = "realtime_ops_mig";
        var connectionString = PostgresConnectionString;
        var schema = new RealtimeDatabaseSchema($"{schemaName}_{_schemaSuffix}");
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await SeedMessagesWithoutConversationIdAsync(client, schema, count: 8);

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            await new RealtimeSchemaMigrationRunner(
                    schema,
                    NullLogger.Instance,
                    [
                        new Migration001_BaselineSchema(),
                        new Migration005_ConversationFoundation(),
                        new Migration009_ConversationBackfillBatches
                        {
                            BatchSize = 5,
                            MaxBatches = 1
                        }
                    ])
                .MigrateAsync(connection);
        }

        var ops = new NpgsqlRealtimeOpsQueryStore(
            client,
            schema,
            new NoopRealtimeOutboxStore(),
            NullLogger<NpgsqlRealtimeOpsQueryStore>.Instance);

        var progress = await ops.GetMigrationProgressAsync();
        Assert.Contains(progress.Catalog, c => c.Version == 9);
        Assert.DoesNotContain(progress.Applied, a => a.Version == 9);
        Assert.Contains(9, progress.NotFullyAppliedVersions);
        Assert.True(progress.HasDeferredInProgress);
        Assert.NotEmpty(progress.OpenCheckpoints);

        var backlogs = await ops.GetBacklogsAsync();
        Assert.False(backlogs.Migration009Applied);
        Assert.True(backlogs.MessagesMissingConversationIdCount > 0);
        Assert.Contains("ChatApp.Server", backlogs.CleanupNote);
    }

    [Fact]
    public async Task RelationshipProjectionStatus_ReportsDurableCursorAndSnapshotCoverage()
    {
        const string schemaName = "realtime_ops_relationship";
        var schema = new RealtimeDatabaseSchema($"{schemaName}_{_schemaSuffix}");
        var client = new RealtimeDatabaseClient(
            PostgresConnectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            await using var createSchema = new NpgsqlCommand(
                $"CREATE SCHEMA IF NOT EXISTS {schema.QuotedSchema};",
                connection);
            await createSchema.ExecuteNonQueryAsync();
            await new Migration060_ServerRelationshipProjection().ApplyAsync(
                connection, null, schema, CancellationToken.None);
            await new Migration061_RelationshipProjectionSnapshots().ApplyAsync(
                connection, null, schema, CancellationToken.None);
            await new Migration062_RelationshipProjectionRebuilder().ApplyAsync(
                connection, null, schema, CancellationToken.None);
            await new Migration063_RelationshipProjectionHistory().ApplyAsync(
                connection, null, schema, CancellationToken.None);
        }

        var projectionStore = new NpgsqlRelationshipProjectionStore(client, schema);
        const long owner = 9_200_001;
        var delta = new RelationshipProjectionDelta
        {
            EventId = RelationshipEventIdFactory.CreateRelationshipProjectionEventId(
                owner,
                RelationshipProjectionListType.Friends,
                1),
            OwnerUserId = owner,
            ListType = RelationshipProjectionListType.Friends,
            Version = 1,
            Operation = RelationshipProjectionOperation.Upsert,
            ResourceId = "friend-ops",
            SubjectUserId = owner + 1,
            ActorUserId = owner,
            State = "Accepted",
            OccurredAtMs = 1_800_000_000_001
        };
        await projectionStore.ApplyAsync(delta);

        var ops = new NpgsqlRelationshipProjectionOpsQueryStore(
            client,
            schema,
            NullLogger<NpgsqlRelationshipProjectionOpsQueryStore>.Instance);
        var beforeSnapshot = await ops.GetStatusAsync();
        Assert.True(beforeSnapshot.Available);
        Assert.Equal(1, beforeSnapshot.VersionStreamCount);
        Assert.Equal(0, beforeSnapshot.SnapshotBaselineStreamCount);
        Assert.Equal(1, beforeSnapshot.StreamsWithoutSnapshotBaselineCount);
        Assert.Equal(1, beforeSnapshot.ProjectionItemCount);
        Assert.Equal(1, beforeSnapshot.InboxEventCount);

        var items = new[]
        {
            new RelationshipProjectionSnapshotItem
            {
                ResourceId = delta.ResourceId,
                SubjectUserId = delta.SubjectUserId,
                ActorUserId = delta.ActorUserId,
                State = delta.State,
                OccurredAtMs = delta.OccurredAtMs
            }
        };
        var resourceHash = RelationshipProjectionSnapshotHash.Compute(
            items.Select(static item => item.ResourceId));
        await projectionStore.ApplySnapshotAsync(new RelationshipProjectionStreamSnapshot
        {
            SnapshotId = RelationshipEventIdFactory.CreateRelationshipProjectionSnapshotId(
                owner,
                RelationshipProjectionListType.Friends,
                1,
                resourceHash),
            OwnerUserId = owner,
            ListType = RelationshipProjectionListType.Friends,
            Version = 1,
            CapturedAtMs = 1_800_000_000_002,
            ItemCount = items.Length,
            ResourceHash = resourceHash,
            Items = items
        });

        var stateStore = new NpgsqlRelationshipProjectionRebuildStateStore(client, schema);
        var lease = Assert.IsType<RelationshipProjectionRebuildLease>(
            await stateStore.TryAcquireAsync("ops-status", TimeSpan.FromSeconds(30)));
        Assert.True(await stateStore.CommitPageAsync(
            lease,
            owner,
            RelationshipProjectionListType.Friends,
            false,
            TimeSpan.FromSeconds(30)));

        var status = await ops.GetStatusAsync();
        Assert.True(status.Available);
        Assert.True(status.LeaseActive);
        Assert.Equal(owner, status.CursorOwnerUserId);
        Assert.Equal(RelationshipProjectionListType.Friends, status.CursorListType);
        Assert.Equal(1, status.SnapshotBaselineStreamCount);
        Assert.Equal(0, status.StreamsWithoutSnapshotBaselineCount);
        Assert.True(status.GeneratedAtMs >= status.UpdatedAtMs);

        await projectionStore.ApplyAsync(new RelationshipProjectionDelta
        {
            EventId = RelationshipEventIdFactory.CreateRelationshipProjectionEventId(
                owner,
                RelationshipProjectionListType.BlockedUsers,
                1),
            OwnerUserId = owner,
            ListType = RelationshipProjectionListType.BlockedUsers,
            Version = 1,
            Operation = RelationshipProjectionOperation.Upsert,
            ResourceId = "blocked-ops",
            SubjectUserId = owner + 2,
            ActorUserId = owner,
            State = "Blocked",
            OccurredAtMs = 1_800_000_000_003
        });

        var firstPage = await ops.ListStreamsAsync(null, null, 1);
        var first = Assert.Single(firstPage.Items);
        Assert.True(firstPage.HasMore);
        Assert.Equal(RelationshipProjectionListType.Friends, first.ListType);
        Assert.True(first.HasSnapshotBaseline);
        Assert.True(first.IsLocallyContiguous);
        Assert.Equal(0, first.DeltaInboxCountAfterSnapshot);

        var secondPage = await ops.ListStreamsAsync(
            firstPage.NextOwnerUserId,
            firstPage.NextListType,
            1);
        var second = Assert.Single(secondPage.Items);
        Assert.False(secondPage.HasMore);
        Assert.Equal(RelationshipProjectionListType.BlockedUsers, second.ListType);
        Assert.False(second.HasSnapshotBaseline);
        Assert.True(second.IsLocallyContiguous);
        Assert.Equal(1, second.DeltaInboxCountAfterSnapshot);

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            await using var removeInbox = new NpgsqlCommand(
                $"""
                 DELETE FROM {schema.RelationshipProjectionInboxTableSql}
                 WHERE "owner_user_id" = @owner AND "list_type" = @list_type;
                 """,
                connection);
            removeInbox.Parameters.AddWithValue("owner", owner);
            removeInbox.Parameters.AddWithValue(
                "list_type",
                (short)RelationshipProjectionListType.BlockedUsers);
            Assert.Equal(1, await removeInbox.ExecuteNonQueryAsync());
        }

        var gapPage = await ops.ListStreamsAsync(
            firstPage.NextOwnerUserId,
            firstPage.NextListType,
            1);
        var gap = Assert.Single(gapPage.Items);
        Assert.False(gap.IsLocallyContiguous);
        Assert.Equal(0, gap.DeltaInboxCountAfterSnapshot);
    }

    private static async Task SeedMessagesWithoutConversationIdAsync(
        RealtimeDatabaseClient client,
        RealtimeDatabaseSchema schema,
        int count)
    {
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await using var createSchema = new NpgsqlCommand(
            $"CREATE SCHEMA IF NOT EXISTS {schema.QuotedSchema};",
            connection);
        await createSchema.ExecuteNonQueryAsync();

        await new Migration001_BaselineSchema().ApplyAsync(
            connection, null, schema, CancellationToken.None);
        await new Migration005_ConversationFoundation().ApplyAsync(
            connection, null, schema, CancellationToken.None);

        for (var i = 0; i < count; i++)
        {
            await using var insert = new NpgsqlCommand(
                $"""
                 INSERT INTO {schema.MessagesTableSql} (
                     message_id, client_message_id, sender_user_id, sender_session_id,
                     receiver_user_id, conversation_id, content, received_at_ms, created_at_ms
                 ) VALUES (
                     @message_id, @client_message_id, 10, 's', 20, NULL, @content, @at, @at
                 );
                 """,
                connection);
            insert.Parameters.AddWithValue("message_id", $"ops-m-{i:D3}");
            insert.Parameters.AddWithValue("client_message_id", $"ops-c-{i:D3}");
            insert.Parameters.AddWithValue("content", $"msg-{i}");
            insert.Parameters.AddWithValue("at", 1_000L + i);
            await insert.ExecuteNonQueryAsync();
        }
    }
}

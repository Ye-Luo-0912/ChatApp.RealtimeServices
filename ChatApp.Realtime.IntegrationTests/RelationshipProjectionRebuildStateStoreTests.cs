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
public sealed class RelationshipProjectionRebuildStateStoreTests(
    RealtimePipelineFixture fixture)
{
    [Fact]
    public async Task Lease_Cursor_FailureResume_AndStablePassesAreFencedAndDurable()
    {
        var schema = new RealtimeDatabaseSchema("realtime");
        await ResetStateAsync(schema);
        await using var client = new RealtimeDatabaseClient(
            fixture.PostgresConnectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        var store = new NpgsqlRelationshipProjectionRebuildStateStore(client, schema);
        var leaseDuration = TimeSpan.FromSeconds(10);

        var first = Assert.IsType<RelationshipProjectionRebuildLease>(
            await store.TryAcquireAsync("instance-a", leaseDuration));
        Assert.Null(await store.TryAcquireAsync("instance-b", leaseDuration));
        Assert.True(await store.CommitPageAsync(
            first,
            42,
            RelationshipProjectionListType.Friends,
            pageChanged: true,
            leaseDuration));
        Assert.True(await store.ReleaseFailureAsync(
            first,
            "source_transport_failed",
            TimeSpan.Zero));

        var resumed = Assert.IsType<RelationshipProjectionRebuildLease>(
            await store.TryAcquireAsync("instance-b", leaseDuration));
        Assert.Equal(42, resumed.AfterOwnerUserId);
        Assert.Equal(RelationshipProjectionListType.Friends, resumed.AfterListType);
        Assert.True(resumed.PassChanged);
        Assert.False(await store.RenewAsync(first, leaseDuration));

        var changedPass = Assert.IsType<RelationshipProjectionRebuildPassResult>(
            await store.CompletePassAsync(
                resumed,
                finalPageChanged: false,
                TimeSpan.FromMinutes(5)));
        Assert.Equal(1, changedPass.PassNumber);
        Assert.Equal(0, changedPass.StablePasses);

        var stableOneLease = Assert.IsType<RelationshipProjectionRebuildLease>(
            await store.TryAcquireAsync("instance-c", leaseDuration));
        Assert.Null(stableOneLease.AfterOwnerUserId);
        var stableOne = Assert.IsType<RelationshipProjectionRebuildPassResult>(
            await store.CompletePassAsync(
                stableOneLease,
                finalPageChanged: false,
                TimeSpan.FromMinutes(5)));
        Assert.Equal(1, stableOne.StablePasses);

        var stableTwoLease = Assert.IsType<RelationshipProjectionRebuildLease>(
            await store.TryAcquireAsync("instance-d", leaseDuration));
        var stableTwo = Assert.IsType<RelationshipProjectionRebuildPassResult>(
            await store.CompletePassAsync(
                stableTwoLease,
                finalPageChanged: false,
                TimeSpan.FromMinutes(5)));
        Assert.Equal(2, stableTwo.StablePasses);
        Assert.True(stableTwo.NextAttemptAtMs > DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        Assert.Null(await store.TryAcquireAsync("instance-e", leaseDuration));
    }

    private async Task ResetStateAsync(RealtimeDatabaseSchema schema)
    {
        await using var connection = new NpgsqlConnection(fixture.PostgresConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"""
             UPDATE {schema.RelationshipProjectionRebuildStateTableSql}
             SET "after_owner_user_id" = NULL,
                 "after_list_type" = NULL,
                 "pass_number" = 0,
                 "pass_changed" = FALSE,
                 "stable_passes" = 0,
                 "lease_owner" = NULL,
                 "claim_token" = NULL,
                 "locked_until_ms" = NULL,
                 "next_attempt_at_ms" = 0,
                 "last_error" = NULL,
                 "updated_at_ms" = 0
             WHERE "id" = 1;
             """,
            connection);
        Assert.Equal(1, await command.ExecuteNonQueryAsync());
    }
}

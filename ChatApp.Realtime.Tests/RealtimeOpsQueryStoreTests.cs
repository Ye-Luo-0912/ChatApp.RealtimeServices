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
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task MigrationProgress_ReportsOpenCheckpoint_When009Deferred()
    {
        const string schemaName = "realtime_ops_mig";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
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

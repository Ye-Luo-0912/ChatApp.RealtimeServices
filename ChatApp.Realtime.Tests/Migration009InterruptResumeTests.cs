using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class Migration009InterruptResumeTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task Migration009_ResumesFromCheckpoint_AfterMaxBatchesInterrupt()
    {
        const string schemaName = "realtime_mig009_resume";
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await SeedMessagesWithoutConversationIdAsync(client, schema, count: 12);

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

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            var applied = await CountAppliedAsync(connection, schema, version: 9);
            Assert.Equal(0, applied);

            var filled = await CountMessagesWithConversationIdAsync(connection, schema);
            Assert.Equal(5, filled);
        }

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            await new RealtimeSchemaMigrationRunner(
                    schema,
                    NullLogger.Instance,
                    [
                        new Migration001_BaselineSchema(),
                        new Migration005_ConversationFoundation(),
                        new Migration009_ConversationBackfillBatches { BatchSize = 5 }
                    ])
                .MigrateAsync(connection);
        }

        await using (var connection = await client.GetDataSource().OpenConnectionAsync())
        {
            var applied = await CountAppliedAsync(connection, schema, version: 9);
            Assert.Equal(1, applied);

            var filled = await CountMessagesWithConversationIdAsync(connection, schema);
            Assert.Equal(12, filled);

            var members = await CountMembersAsync(connection, schema);
            Assert.True(members >= 2);
        }
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
            connection,
            null,
            schema,
            CancellationToken.None);
        await new Migration005_ConversationFoundation().ApplyAsync(
            connection,
            null,
            schema,
            CancellationToken.None);

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
            insert.Parameters.AddWithValue("message_id", $"m-{i:D3}");
            insert.Parameters.AddWithValue("client_message_id", $"c-{i:D3}");
            insert.Parameters.AddWithValue("content", $"msg-{i}");
            insert.Parameters.AddWithValue("at", 1_000L + i);
            await insert.ExecuteNonQueryAsync();
        }
    }

    private static async Task<int> CountAppliedAsync(
        NpgsqlConnection connection,
        RealtimeDatabaseSchema schema,
        int version)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.SchemaMigrationsTableSql} WHERE version = @version;",
            connection);
        command.Parameters.AddWithValue("version", version);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async Task<int> CountMessagesWithConversationIdAsync(
        NpgsqlConnection connection,
        RealtimeDatabaseSchema schema)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.MessagesTableSql} WHERE conversation_id IS NOT NULL;",
            connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }

    private static async Task<int> CountMembersAsync(
        NpgsqlConnection connection,
        RealtimeDatabaseSchema schema)
    {
        await using var command = new NpgsqlCommand(
            $"SELECT COUNT(*) FROM {schema.ConversationMembersTableSql};",
            connection);
        return Convert.ToInt32(await command.ExecuteScalarAsync());
    }
}

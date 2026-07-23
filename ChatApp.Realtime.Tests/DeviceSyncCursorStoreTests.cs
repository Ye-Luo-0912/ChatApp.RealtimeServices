using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class DeviceSyncCursorStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task UpsertAndLoad_AdvancesMonotonicallyPerDevice()
    {
        var (client, schema) = await CreateSchemaAsync("realtime_device_cursors");
        var store = new NpgsqlRealtimeDeviceSyncCursorStore(client, schema);

        await store.UpsertManyAsync(
            42,
            7,
            [
                new DeviceSyncCursor
                {
                    ConversationId = "dm:42:43",
                    AfterReceivedAtMs = 100,
                    AfterMessageId = "msg-1"
                }
            ]);

        await store.UpsertManyAsync(
            42,
            7,
            [
                new DeviceSyncCursor
                {
                    ConversationId = "dm:42:43",
                    AfterReceivedAtMs = 50,
                    AfterMessageId = "msg-0"
                }
            ]);

        await store.UpsertManyAsync(
            42,
            7,
            [
                new DeviceSyncCursor
                {
                    ConversationId = "dm:42:43",
                    AfterReceivedAtMs = 200,
                    AfterMessageId = "msg-2"
                }
            ]);

        var loaded = await store.LoadAsync(42, 7, take: 10);
        var cursor = Assert.Single(loaded);
        Assert.Equal(200, cursor.AfterReceivedAtMs);
        Assert.Equal("msg-2", cursor.AfterMessageId);

        var otherDevice = await store.LoadAsync(42, 8, take: 10);
        Assert.Empty(otherDevice);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateSchemaAsync(
        string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }
}

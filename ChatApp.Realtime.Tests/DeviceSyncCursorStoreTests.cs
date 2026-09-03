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
                    AfterChangedAtMs = 100,
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
                    AfterChangedAtMs = 50,
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
                    AfterChangedAtMs = 200,
                    AfterMessageId = "msg-2"
                }
            ]);

        var loaded = await store.LoadAsync(42, 7, take: 10);
        var cursor = Assert.Single(loaded);
        Assert.Equal(200, cursor.AfterChangedAtMs);
        Assert.Equal("msg-2", cursor.AfterMessageId);

        var otherDevice = await store.LoadAsync(42, 8, take: 10);
        Assert.Empty(otherDevice);
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateSchemaAsync(
        string schemaName)
    {
        var connectionString = PostgresConnectionString;
        var schema = new RealtimeDatabaseSchema($"{schemaName}_{_schemaSuffix}");
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);
        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);
        return (client, schema);
    }
}

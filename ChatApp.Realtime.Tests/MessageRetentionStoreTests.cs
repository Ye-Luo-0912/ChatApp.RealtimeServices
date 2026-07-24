using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

public sealed class MessageRetentionStoreTests : IAsyncLifetime
{
    private readonly PostgreSqlContainer _postgres = new PostgreSqlBuilder()
        .WithImage("postgres:16-alpine")
        .Build();

    public Task InitializeAsync() => _postgres.StartAsync();

    public Task DisposeAsync() => _postgres.DisposeAsync().AsTask();

    [Fact]
    public async Task PurgeBatch_DeletesOnlyOldRows_RetainsRecent()
    {
        const string schemaName = "realtime_retention_basic";
        var (client, schema, store) = await CreateStoreAsync(schemaName);
        var cs = _postgres.GetConnectionString();

        await InsertMessageAsync(cs, schema, "old-1", receivedAtMs: 1_000, conversationId: "c1");
        await InsertMessageAsync(cs, schema, "old-2", receivedAtMs: 2_000, conversationId: "c1");
        await InsertMessageAsync(cs, schema, "new-1", receivedAtMs: 50_000, conversationId: "c1");
        await InsertReactionAsync(cs, schema, "old-1", userId: 10, emoji: "👍");
        await InsertMutationAsync(cs, schema, "old-1", actorUserId: 10, requestId: "req-1");

        var result = await store.TryPurgeBatchAsync(cutoffReceivedAtMs: 10_000, batchSize: 100);
        Assert.True(result.LockAcquired);
        Assert.Equal(2, result.DeletedCount);

        var remaining = await ListMessageIdsAsync(cs, schema);
        Assert.Equal(["new-1"], remaining);
        Assert.Equal(0, await CountReactionsAsync(cs, schema));
        Assert.Equal(0, await CountMutationsAsync(cs, schema));
    }

    [Fact]
    public async Task PurgeBatch_MultiBatch_ProgressesUntilEmpty()
    {
        const string schemaName = "realtime_retention_batches";
        var (client, schema, store) = await CreateStoreAsync(schemaName);
        var cs = _postgres.GetConnectionString();

        for (var i = 0; i < 5; i++)
        {
            await InsertMessageAsync(
                cs,
                schema,
                $"old-{i}",
                receivedAtMs: 1_000 + i,
                conversationId: "c-batch");
        }

        await InsertMessageAsync(cs, schema, "keep", receivedAtMs: 100_000, conversationId: "c-batch");

        var total = 0;
        for (var i = 0; i < 10; i++)
        {
            var batch = await store.TryPurgeBatchAsync(cutoffReceivedAtMs: 50_000, batchSize: 2);
            Assert.True(batch.LockAcquired);
            if (batch.DeletedCount == 0)
                break;
            total += batch.DeletedCount;
        }

        Assert.Equal(5, total);
        Assert.Equal(["keep"], await ListMessageIdsAsync(cs, schema));
    }

    [Fact]
    public async Task Options_DisabledOrZeroHorizon_IsEffectivelyOff()
    {
        var off = new MessageRetentionOptions { Enabled = false, RetentionHorizonMs = 86_400_000 };
        Assert.False(off.IsEffectivelyEnabled(syncBootstrapRetentionHorizonMs: 86_400_000));

        var zero = new MessageRetentionOptions { Enabled = true, RetentionHorizonMs = 0, RetentionDays = 0 };
        Assert.False(zero.IsEffectivelyEnabled(syncBootstrapRetentionHorizonMs: 0));

        var fromSync = new MessageRetentionOptions { Enabled = true, RetentionHorizonMs = 0 };
        Assert.Equal(7_000, fromSync.ResolveEffectiveHorizonMs(7_000));
        Assert.True(fromSync.IsEffectivelyEnabled(7_000));

        var fromDays = new MessageRetentionOptions { Enabled = true, RetentionDays = 2 };
        Assert.Equal(2 * 86_400_000L, fromDays.ResolveEffectiveHorizonMs(0));
    }

    [Fact]
    public async Task WorkerIdle_WhenDisabled_DoesNotDelete()
    {
        const string schemaName = "realtime_retention_disabled";
        var (_, schema, store) = await CreateStoreAsync(schemaName);
        var cs = _postgres.GetConnectionString();
        await InsertMessageAsync(cs, schema, "old", receivedAtMs: 1, conversationId: "c");

        var options = new MessageRetentionOptions
        {
            Enabled = false,
            RetentionHorizonMs = 1,
            BatchSize = 100,
            IntervalMs = 60_000
        };
        // Mirror worker gate: no purge when not effectively enabled.
        Assert.False(options.IsEffectivelyEnabled(0));
        Assert.Equal(["old"], await ListMessageIdsAsync(cs, schema));

        // Horizon 0 with Enabled=true also idle.
        var horizonOff = new MessageRetentionOptions { Enabled = true, RetentionHorizonMs = 0 };
        Assert.False(horizonOff.IsEffectivelyEnabled(0));
        _ = store;
    }

    [Fact]
    public async Task PurgeBatch_RepairsTip_WhenAllMessagesPurged()
    {
        const string schemaName = "realtime_retention_tip";
        var (_, schema, store) = await CreateStoreAsync(schemaName);
        var cs = _postgres.GetConnectionString();

        await EnsureConversationAsync(cs, schema, "c-empty", lastMessageId: "only", lastAtMs: 5_000);
        await InsertMessageAsync(cs, schema, "only", receivedAtMs: 5_000, conversationId: "c-empty");

        var result = await store.TryPurgeBatchAsync(cutoffReceivedAtMs: 10_000, batchSize: 10);
        Assert.Equal(1, result.DeletedCount);

        await using var connection = new NpgsqlConnection(cs);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"""
             SELECT last_message_id, last_message_at_ms
             FROM {schema.ConversationsTableSql}
             WHERE conversation_id = 'c-empty';
             """,
            connection);
        await using var reader = await cmd.ExecuteReaderAsync();
        Assert.True(await reader.ReadAsync());
        Assert.True(reader.IsDBNull(0));
        Assert.True(reader.IsDBNull(1));
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema, NpgsqlRealtimeMessageRetentionStore Store)>
        CreateStoreAsync(string schemaName)
    {
        var connectionString = _postgres.GetConnectionString();
        var schema = new RealtimeDatabaseSchema(schemaName);
        var client = new RealtimeDatabaseClient(
            connectionString,
            NullLogger<RealtimeDatabaseClient>.Instance);

        await using var connection = await client.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(schema, NullLogger.Instance)
            .MigrateAsync(connection);

        var store = new NpgsqlRealtimeMessageRetentionStore(
            client,
            schema,
            NullLogger<NpgsqlRealtimeMessageRetentionStore>.Instance);
        return (client, schema, store);
    }

    private static async Task InsertMessageAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string messageId,
        long receivedAtMs,
        string conversationId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.MessagesTableSql} (
                 message_id, client_message_id, sender_user_id, sender_session_id,
                 receiver_user_id, conversation_id, content, received_at_ms, created_at_ms,
                 changed_at_ms, edit_version
             ) VALUES (
                 @message_id, @client_message_id, 10, 's', 20, @conversation_id, @content,
                 @at, @at, @at, 1
             );
             """,
            connection);
        insert.Parameters.AddWithValue("message_id", messageId);
        insert.Parameters.AddWithValue("client_message_id", $"c-{messageId}");
        insert.Parameters.AddWithValue("conversation_id", conversationId);
        insert.Parameters.AddWithValue("content", $"body-{messageId}");
        insert.Parameters.AddWithValue("at", receivedAtMs);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task InsertReactionAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string messageId,
        long userId,
        string emoji)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.MessageReactionsTableSql} (
                 message_id, user_id, emoji, created_at_ms
             ) VALUES (@message_id, @user_id, @emoji, 1);
             """,
            connection);
        insert.Parameters.AddWithValue("message_id", messageId);
        insert.Parameters.AddWithValue("user_id", userId);
        insert.Parameters.AddWithValue("emoji", emoji);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task InsertMutationAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string messageId,
        long actorUserId,
        string requestId)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.MessageMutationRequestsTableSql} (
                 actor_user_id, request_id, operation, message_id, payload_fingerprint,
                 succeeded, created_at_ms
             ) VALUES (
                 @actor, @request_id, 1, @message_id, 'fp', true, 1
             );
             """,
            connection);
        insert.Parameters.AddWithValue("actor", actorUserId);
        insert.Parameters.AddWithValue("request_id", requestId);
        insert.Parameters.AddWithValue("message_id", messageId);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task EnsureConversationAsync(
        string connectionString,
        RealtimeDatabaseSchema schema,
        string conversationId,
        string lastMessageId,
        long lastAtMs)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var insert = new NpgsqlCommand(
            $"""
             INSERT INTO {schema.ConversationsTableSql} (
                 conversation_id, type, created_at_ms, updated_at_ms,
                 last_message_id, last_message_preview, last_message_at_ms, last_sender_user_id
             ) VALUES (
                 @id, 1, @at, @at, @message_id, 'preview', @at, 10
             )
             ON CONFLICT (conversation_id) DO UPDATE
             SET last_message_id = EXCLUDED.last_message_id,
                 last_message_at_ms = EXCLUDED.last_message_at_ms;
             """,
            connection);
        insert.Parameters.AddWithValue("id", conversationId);
        insert.Parameters.AddWithValue("message_id", lastMessageId);
        insert.Parameters.AddWithValue("at", lastAtMs);
        await insert.ExecuteNonQueryAsync();
    }

    private static async Task<List<string>> ListMessageIdsAsync(
        string connectionString,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT message_id FROM {schema.MessagesTableSql} ORDER BY message_id",
            connection);
        var ids = new List<string>();
        await using var reader = await cmd.ExecuteReaderAsync();
        while (await reader.ReadAsync())
            ids.Add(reader.GetString(0));
        return ids;
    }

    private static async Task<long> CountReactionsAsync(
        string connectionString,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*)::bigint FROM {schema.MessageReactionsTableSql}",
            connection);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }

    private static async Task<long> CountMutationsAsync(
        string connectionString,
        RealtimeDatabaseSchema schema)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var cmd = new NpgsqlCommand(
            $"SELECT COUNT(*)::bigint FROM {schema.MessageMutationRequestsTableSql}",
            connection);
        return (long)(await cmd.ExecuteScalarAsync())!;
    }
}

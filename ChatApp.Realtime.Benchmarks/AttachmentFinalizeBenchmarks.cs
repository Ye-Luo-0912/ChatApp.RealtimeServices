using BenchmarkDotNet.Attributes;
using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Core.Diagnostics;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Benchmarks;

/// <summary>
/// 门禁4：Attachment Finalize 固定 SQL 次数断言。
/// <para>
/// FinalizeUploadAsync 必须走单条 <c>UPDATE ... RETURNING</c>（1 SQL），
/// 不允许引入额外校验往返或状态回查。若 SQL 次数超过 1，说明 Finalize 路径
/// 出现了多余连接/查询，基准会抛异常使门禁失败。
/// </para>
/// </summary>
[MemoryDiagnoser]
[SimpleJob(warmupCount: 3, iterationCount: 5)]
public class AttachmentFinalizeBenchmarks
{
    private const int SqlFinalize = 1;

    private const long UploaderUserId = 7001;
    private const string AttachmentId = "bench-att-0001";
    private const string ConversationId = "bench-finalize-conv-001";

    private PostgreSqlContainer? _container;
    private RealtimeDatabaseClient? _dbClient;
    private RealtimeDatabaseSchema? _schema;
    private NpgsqlConnection? _connection;
    private NpgsqlRealtimeAttachmentStore? _store;

    [GlobalSetup]
    public void Initialize()
    {
        InitializeAsync().GetAwaiter().GetResult();
    }

    private async Task InitializeAsync()
    {
        _container = new PostgreSqlBuilder().WithImage("postgres:16-alpine").Build();
        await _container.StartAsync();

        _dbClient = new RealtimeDatabaseClient(
            _container.GetConnectionString(),
            NullLogger<RealtimeDatabaseClient>.Instance);
        _schema = new RealtimeDatabaseSchema("realtime_bench_att");

        await using var migrateConnection = await _dbClient.GetDataSource().OpenConnectionAsync();
        await new RealtimeSchemaMigrationRunner(_schema, NullLogger<RealtimeSchemaMigrationRunner>.Instance)
            .MigrateAsync(migrateConnection);

        _connection = new NpgsqlConnection(_container.GetConnectionString());
        await _connection.OpenAsync();

        _store = new NpgsqlRealtimeAttachmentStore(
            _dbClient,
            _schema,
            NullLogger<NpgsqlRealtimeAttachmentStore>.Instance);
    }

    [GlobalCleanup]
    public void Cleanup()
    {
        CleanupAsync().GetAwaiter().GetResult();
    }

    private async Task CleanupAsync()
    {
        if (_connection is not null)
            await _connection.DisposeAsync();
        if (_dbClient is not null)
            await _dbClient.DisposeAsync();
        if (_container is not null)
            await _container.DisposeAsync();
    }

    /// <summary>每次迭代重置为一条 Ticketed 附件记录，供 Finalize 消费。</summary>
    [IterationSetup]
    public void SetupIteration()
    {
        SetupIterationCore().GetAwaiter().GetResult();
    }

    private async Task SetupIterationCore()
    {
        var schema = _schema!;
        var nowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        await using var reset = new NpgsqlCommand(
            $"""
             TRUNCATE {schema.AttachmentsTableSql};
             TRUNCATE {schema.ConversationsTableSql};
             INSERT INTO {schema.ConversationsTableSql}
                 (conversation_id, type, created_at_ms, updated_at_ms)
             VALUES (@conv, 1, @now, @now)
             ON CONFLICT DO NOTHING;
             INSERT INTO {schema.AttachmentsTableSql}
                 (attachment_id, uploader_user_id, object_key, content_type, size_bytes,
                  status, created_at_ms, state_version)
             VALUES (@att, @user, 'objects/0001.bin', 'application/octet-stream', 0,
                     @ticketed, @now, 0)
             ON CONFLICT DO NOTHING;
             """,
            _connection!);
        reset.Parameters.AddWithValue("conv", ConversationId);
        reset.Parameters.AddWithValue("now", nowMs);
        reset.Parameters.AddWithValue("att", AttachmentId);
        reset.Parameters.AddWithValue("user", UploaderUserId);
        reset.Parameters.AddWithValue("ticketed", (short)AttachmentStatus.Ticketed);
        await reset.ExecuteNonQueryAsync();
    }

    [Benchmark(Description = "Attachment FinalizeUpload (1 SQL, Ticketed→Uploaded)")]
    public async Task<int> FinalizeUpload()
    {
        using var scope = NpgsqlSqlCommandCounter.BeginScope();
        var result = await _store!.FinalizeUploadAsync(
            actorUserId: UploaderUserId,
            attachmentId: AttachmentId,
            sizeBytes: 2048,
            contentHash: "abc123");
        return AssertSqlCount(SqlFinalize);
    }

    private int AssertSqlCount(int upperBound)
    {
        var count = NpgsqlSqlCommandCounter.GetCommandCount();
        if (count > upperBound)
        {
            throw new InvalidOperationException(
                $"SQL 命令数 {count} 超过门禁上限 {upperBound}。Finalize 必须为固定单条 UPDATE，不得引入额外往返。");
        }
        return count;
    }
}
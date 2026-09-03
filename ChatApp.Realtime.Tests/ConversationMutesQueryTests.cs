using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Conversations;
using ChatApp.Realtime.Infrastructure.Core.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using ChatApp.Realtime.Infrastructure.Postgres.Migrations;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;
using ChatApp.Realtime.Tests.TestDoubles;
using Microsoft.Extensions.Logging.Abstractions;
using Testcontainers.PostgreSql;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// 会话免打扰批量查询（ACCOUNT-OPS-1 离线推送过滤）：
/// store 生效语义（is_muted + muted_until_ms 窗口）与 processor 校验。
/// </summary>
public sealed class ConversationMutesQueryTests : IAsyncLifetime
{
    // 固定时间锚点：1_000_000_000_000 = 2001-09（过去）；4_000_000_000_000 = 2096-02（未来）。
    private const long PastUntilMs = 1_000_000_000_000L;
    private const long FutureUntilMs = 4_000_000_000_000L;

    private readonly PostgreSqlContainer? _postgres = string.IsNullOrEmpty(ExternalPostgresConnectionString()) ? new PostgreSqlBuilder(
        "postgres:16-alpine")
        .Build() : null;
    private readonly string _externalPostgres = ExternalPostgresConnectionString() ?? string.Empty;
    private readonly string _schemaSuffix = Guid.NewGuid().ToString("N")[..8];

    private static string? ExternalPostgresConnectionString() => Environment.GetEnvironmentVariable("CHATAPP_TEST_POSTGRES");

    private string PostgresConnectionString => _postgres?.GetConnectionString() ?? _externalPostgres;

    public Task InitializeAsync() => _postgres?.StartAsync() ?? Task.CompletedTask;

    public Task DisposeAsync() => _postgres?.DisposeAsync().AsTask() ?? Task.CompletedTask;

    [Fact]
    public async Task QueryMutedMemberIds_ReturnsOnlyEffectiveMutes_Ascending()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_mutes_query");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            outboxSignal);

        // 群：601（创建者）+ 602/603/604。
        var conversationId = ConversationId.CreateGroup();
        var created = await groupStore.CreateGroupAsync(
            "req-mutes-create",
            601,
            conversationId,
            "Mutes",
            [602, 603, 604],
            "s1",
            1_700_000_000_000);
        Assert.True(created.Succeeded);

        // 601：限时免打扰（未来截止 → 生效）。
        Assert.True((await prefsProcessor.ProcessAsync(new ConversationSetPrefsCommand
        {
            RequestId = "mute-601",
            UserId = 601,
            ConversationId = conversationId,
            Muted = true,
            MutedUntilMs = FutureUntilMs
        })).Succeeded);
        // 602：永久免打扰（until null → 生效）。
        Assert.True((await prefsProcessor.ProcessAsync(new ConversationSetPrefsCommand
        {
            RequestId = "mute-602",
            UserId = 602,
            ConversationId = conversationId,
            Muted = true
        })).Succeeded);
        // 603：过期免打扰（截止在过去 → 不生效）。
        Assert.True((await prefsProcessor.ProcessAsync(new ConversationSetPrefsCommand
        {
            RequestId = "mute-603",
            UserId = 603,
            ConversationId = conversationId,
            Muted = true,
            MutedUntilMs = PastUntilMs
        })).Succeeded);

        // 乱序输入 + 未静音成员（604）+ 非成员（9999）：仅返回生效免打扰，升序。
        var muted = await conversationStore.QueryMutedMemberIdsAsync(
            conversationId,
            [9999, 604, 603, 602, 601]);
        Assert.Equal(new long[] { 601, 602 }, muted);

        // 取消 602 免打扰后不再返回。
        Assert.True((await prefsProcessor.ProcessAsync(new ConversationSetPrefsCommand
        {
            RequestId = "unmute-602",
            UserId = 602,
            ConversationId = conversationId,
            Muted = false
        })).Succeeded);
        var mutedAfterUnmute = await conversationStore.QueryMutedMemberIdsAsync(
            conversationId,
            [601, 602, 603, 604]);
        Assert.Equal(new long[] { 601 }, mutedAfterUnmute);

        // 不存在的会话：空结果。
        var unknown = await conversationStore.QueryMutedMemberIdsAsync(
            ConversationId.CreateGroup(),
            [601, 602]);
        Assert.Empty(unknown);
    }

    [Fact]
    public async Task Processor_QueriesStore_AndReturnsMutedOnly()
    {
        var (client, schema) = await CreateDatabaseAsync("realtime_mutes_processor");
        var groupStore = new NpgsqlRealtimeGroupStore(client, schema);
        var conversationStore = new NpgsqlRealtimeConversationStore(client, schema);
        var outboxSignal = new RecordingRealtimeOutboxSignal();
        var prefsProcessor = new DefaultConversationSetPrefsProcessor(
            conversationStore,
            outboxSignal);
        var mutesProcessor = new DefaultConversationMutesQueryProcessor(conversationStore);

        var conversationId = ConversationId.CreateGroup();
        await groupStore.CreateGroupAsync(
            "req-mutes-proc-create",
            701,
            conversationId,
            "MutesProc",
            [702, 703],
            "s1",
            1_700_000_000_000);
        Assert.True((await prefsProcessor.ProcessAsync(new ConversationSetPrefsCommand
        {
            RequestId = "mute-703",
            UserId = 703,
            ConversationId = conversationId,
            Muted = true
        })).Succeeded);

        var result = await mutesProcessor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "mutes-q1",
            ConversationId = conversationId,
            MemberUserIds = [702, 703, 701]
        });

        Assert.True(result.Succeeded);
        Assert.Equal("mutes-q1", result.RequestId);
        Assert.Equal(new long[] { 703 }, result.MutedUserIds);
    }

    [Fact]
    public async Task Processor_ValidatesInput_BeforeStore()
    {
        // Noop store 对任何查询都会抛异常：校验失败的用例证明短路发生在 store 之前。
        var processor = new DefaultConversationMutesQueryProcessor(new NoopRealtimeConversationStore());

        var emptyMembers = await processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "q-empty",
            ConversationId = ConversationId.CreateGroup(),
            MemberUserIds = []
        });
        Assert.False(emptyMembers.Succeeded);
        Assert.Equal("invalid_member_user_ids", emptyMembers.ErrorCode);

        var blankConversation = await processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "q-blank-conv",
            ConversationId = "  ",
            MemberUserIds = [1]
        });
        Assert.False(blankConversation.Succeeded);
        Assert.Equal("invalid_conversation_id", blankConversation.ErrorCode);

        var nonPositiveMember = await processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "q-bad-member",
            ConversationId = ConversationId.CreateGroup(),
            MemberUserIds = [10, 0]
        });
        Assert.False(nonPositiveMember.Succeeded);
        Assert.Equal("invalid_member_user_ids", nonPositiveMember.ErrorCode);

        var tooLongRequestId = await processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = new string('r', 65),
            ConversationId = ConversationId.CreateGroup(),
            MemberUserIds = [10]
        });
        Assert.False(tooLongRequestId.Succeeded);
        Assert.Equal("invalid_request_id", tooLongRequestId.ErrorCode);

        var tooManyMembers = await processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "q-too-many",
            ConversationId = ConversationId.CreateGroup(),
            MemberUserIds = Enumerable.Range(1, DefaultConversationMutesQueryProcessor.MaxMemberUserIds + 1)
                .Select(i => (long)i)
                .ToArray()
        });
        Assert.False(tooManyMembers.Succeeded);
        Assert.Equal("too_many_member_user_ids", tooManyMembers.ErrorCode);

        // 校验通过但存储不可用（Noop）：异常向worker传播，由worker回查询失败响应
        //（Gateway 侧 fail-open，不过滤）。
        await Assert.ThrowsAsync<InvalidOperationException>(() => processor.ProcessAsync(new ConversationMutesQuery
        {
            RequestId = "q-noop",
            ConversationId = ConversationId.CreateGroup(),
            MemberUserIds = [10]
        }));
    }

    private async Task<(RealtimeDatabaseClient Client, RealtimeDatabaseSchema Schema)> CreateDatabaseAsync(
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

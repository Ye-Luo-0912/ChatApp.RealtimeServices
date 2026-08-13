using System.Text.Json;
using System.Threading.Channels;
using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Calls;
using ChatApp.Realtime.IntegrationTests.Fixtures;
using ChatApp.Realtime.Infrastructure.Core.Serialization;
using NATS.Client.Core;
using NATS.Client.JetStream;
using Npgsql;

namespace ChatApp.Realtime.IntegrationTests;

/// <summary>
/// CALL-CTRL-1：经宿主 DI + Core NATS 零持久化信令路径驱动完整通话生命周期。
/// <para>
/// 覆盖：
/// - 主叫 Invite → 被叫 Accept → 主叫 End 的完整闭环，状态收敛（Ringing/Active/Ended）；
/// - SDP 仅经受预算限制的临时信令路径（Core NATS 的 chat.call-signals）转发给对端；
/// - 幂等：同一 command id 重放返回一致结果，不产生重复信令；
/// - 授权 fail-closed：身份头不匹配 / grant 过期 / 非参与方 均被拒绝返回稳定错误码；
/// - 零持久化：通话信令不进入 JetStream 流，也不进入 PostgreSQL（无 call 表）。
/// 音频媒体由 WebRTC/STUN/TURN/SFU 承载，Realtime 不转发 UDP 音频包。
/// </para>
/// </summary>
[Collection(nameof(RealtimePipelineCollection))]
public sealed class CallControlLifecycleTests
{
    private const string CallCommandsSubject = "chat.call-commands";
    private const string CallSignalsSubject = "chat.call-signals";
    private const long Caller = 9_100_000_101;
    private const long Callee = 9_100_000_102;

    private readonly RealtimePipelineFixture _fixture;

    public CallControlLifecycleTests(RealtimePipelineFixture fixture) => _fixture = fixture;

    [Fact]
    public async Task FullLifecycle_InviteAcceptEnd_ConvergesAndForwardsSdpViaCoreNats_WithoutPersistence()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var ct = timeout.Token;
        var callId = $"call-{Guid.NewGuid():N}";
        var grant = Grant(callId);

        await using var probe = new CallSignalProbe(_fixture.NatsUrl);
        await probe.StartAsync(ct);

        // Invite：主叫发起，携带 offer SDP。
        var invite = await SendAsync(callId, CallCommandType.Invite, Caller, revision: 1,
            commandId: "c-invite", sdp: OfferSdp, grant, ct);
        Assert.True(invite.Succeeded, invite.ErrorMessage);
        Assert.Equal(CallState.Ringing, invite.State);

        // Accept：被叫应答，携带 answer SDP。
        var accept = await SendAsync(callId, CallCommandType.Accept, Callee, revision: 2,
            commandId: "c-accept", sdp: AnswerSdp, grant, ct);
        Assert.True(accept.Succeeded, accept.ErrorMessage);
        Assert.Equal(CallState.Active, accept.State);

        // End：主叫挂断 → Ended(HungUp)。
        var end = await SendAsync(callId, CallCommandType.End, Caller, revision: 3,
            commandId: "c-end", grant: grant, ct: ct);
        Assert.True(end.Succeeded, end.ErrorMessage);
        Assert.Equal(CallState.Ended, end.State);
        Assert.Equal(CallEndReason.HungUp, end.EndReason);

        // 信号经临时信令路径转发：offer + answer 均到达 chat.call-signals。
        var signals = probe.TakeAll();
        Assert.Equal(2, signals.Count);
        Assert.All(signals, s => Assert.Equal(callId, s.CallId));
        Assert.Contains(signals, s => s.Kind == CallCommandType.Invite && s.Sdp == OfferSdp);
        Assert.Contains(signals, s => s.Kind == CallCommandType.Accept && s.Sdp == AnswerSdp);

        // 零持久化：不进入 JetStream 流，也不进入 PostgreSQL。
        await AssertCallSignalsNotPersistedAsync(callId, ct);
    }

    [Fact]
    public async Task DuplicateCommandId_ReplaysIdempotently_WithoutDuplicateSignal()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(45));
        var ct = timeout.Token;
        var callId = $"call-{Guid.NewGuid():N}";
        var grant = Grant(callId);

        await using var probe = new CallSignalProbe(_fixture.NatsUrl);
        await probe.StartAsync(ct);

        var invite = new CallCommand
        {
            CommandId = "c-dup",
            CallId = callId,
            Type = CallCommandType.Invite,
            ActorUserId = Caller,
            ActorSessionId = "sess-caller",
            Grant = grant,
            Revision = 1,
            Sdp = OfferSdp
        };

        var first = await SendAsync(invite, Caller, ct);
        Assert.True(first.Succeeded);
        Assert.Equal(CallState.Ringing, first.State);

        // 重放同一 command id：幂等返回，不发生新迁移、不重复转发。
        var replay = await SendAsync(invite, Caller, ct);
        Assert.True(replay.Succeeded);
        Assert.True(replay.Replayed);
        Assert.Equal(first.Revision, replay.Revision);

        var signals = probe.TakeAll();
        Assert.Single(signals);
        Assert.Equal(CallCommandType.Invite, signals[0].Kind);
    }

    [Fact]
    public async Task MismatchedIdentityHeader_IsRejected_FailClosed()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = timeout.Token;
        var callId = $"call-{Guid.NewGuid():N}";
        var grant = Grant(callId);

        var command = new CallCommand
        {
            CommandId = "c-evil",
            CallId = callId,
            Type = CallCommandType.Invite,
            ActorUserId = Caller,
            ActorSessionId = "sess-caller",
            Grant = grant,
            Revision = 1,
            Sdp = OfferSdp
        };

        // 身份头中声明另一个用户（伪造），与实际 actor 不一致 → fail-closed 拒绝。
        var result = await SendAsync(command, trustedUserId: Callee, ct);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.GrantInvalid, result.ErrorCode);
    }

    [Fact]
    public async Task ExpiredGrant_IsRejected_FailClosed()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = timeout.Token;
        var callId = $"call-{Guid.NewGuid():N}";
        var grant = Grant(callId, expiresAtMs: DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - 60_000);

        var command = new CallCommand
        {
            CommandId = "c-expired",
            CallId = callId,
            Type = CallCommandType.Invite,
            ActorUserId = Caller,
            ActorSessionId = "sess-caller",
            Grant = grant,
            Revision = 1,
            Sdp = OfferSdp
        };

        var result = await SendAsync(command, Caller, ct);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.GrantExpired, result.ErrorCode);
    }

    [Fact]
    public async Task NonParticipant_IsRejected_FailClosed()
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var ct = timeout.Token;
        var callId = $"call-{Guid.NewGuid():N}";
        var grant = Grant(callId);

        var command = new CallCommand
        {
            CommandId = "c-outsider",
            CallId = callId,
            Type = CallCommandType.Invite,
            ActorUserId = 9_999_999_999, // 既非主叫也非被叫
            ActorSessionId = "sess-outsider",
            Grant = grant,
            Revision = 1,
            Sdp = OfferSdp
        };

        var result = await SendAsync(command, 9_999_999_999, ct);
        Assert.False(result.Succeeded);
        Assert.Equal(CallErrorCode.GrantInvalid, result.ErrorCode);
    }

    private static string OfferSdp => "v=0\r\no=- 1 1 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 9 RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\n";
    private static string AnswerSdp => "v=0\r\no=- 2 2 IN IP4 127.0.0.1\r\ns=-\r\nm=audio 9 RTP/SAVPF 111\r\na=rtpmap:111 opus/48000/2\r\na=answer\r\n";

    private static CallGrant Grant(string callId, long? expiresAtMs = null) => new()
    {
        CallId = callId,
        CallerUserId = Caller,
        CalleeUserId = Callee,
        ExpiresAtMs = expiresAtMs ?? DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 60_000,
        Nonce = Guid.NewGuid().ToString("N"),
        Signature = "fingerprint"
    };

    private async Task<CallProcessResult> SendAsync(
        string callId,
        CallCommandType type,
        long actor,
        long revision,
        string commandId,
        string? sdp = null,
        CallGrant? grant = null,
        CancellationToken ct = default)
    {
        var command = new CallCommand
        {
            CommandId = commandId,
            CallId = callId,
            Type = type,
            ActorUserId = actor,
            ActorSessionId = "sess-" + actor,
            Grant = grant!,
            Revision = revision,
            Sdp = sdp
        };
        return await SendAsync(command, actor, ct);
    }

    private async Task<CallProcessResult> SendAsync(
        CallCommand command,
        long trustedUserId,
        CancellationToken ct)
    {
        var headers = new NatsHeaders
        {
            [RealtimeIdentityHeaders.UserId] = trustedUserId.ToString(),
            [RealtimeIdentityHeaders.SessionId] = command.ActorSessionId
        };

        await using var real = await OpenConnectionAsync(ct);
        var payload = JsonSerializer.Serialize(command, RealtimeJsonSerializerContext.Default.CallCommand);
        var reply = await real.RequestAsync<string, string>(
                CallCommandsSubject,
                payload,
                headers: headers,
                cancellationToken: ct)
            .ConfigureAwait(false);
        reply.EnsureSuccess();

        return JsonSerializer.Deserialize(
                   reply.Data!,
                   RealtimeJsonSerializerContext.Default.CallProcessResult)
               ?? throw new JsonException("通话信令命令响应无法反序列化。");
    }

    private async Task<NatsConnection> OpenConnectionAsync(CancellationToken ct)
    {
        var connection = new NatsConnection(new NatsOpts
        {
            Url = _fixture.NatsUrl,
            Name = "chatapp-e2e-call-req"
        });
        await connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);
        return connection;
    }

    private async Task AssertCallSignalsNotPersistedAsync(string callId, CancellationToken ct)
    {
        // 1) 不进入 JetStream：不存在覆盖 call-signals/call-commands 的流。
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = _fixture.NatsUrl,
            Name = "chatapp-e2e-call-probe"
        });
        await connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);
        var js = new NatsJSContext(connection);
        var streamSubjects = new List<string>();
        await foreach (var stream in js.ListStreamsAsync(cancellationToken: ct).ConfigureAwait(false))
            streamSubjects.AddRange(stream.Info.Config.Subjects);

        Assert.DoesNotContain(
            streamSubjects,
            s => SubjectMatches(s, CallCommandsSubject) || SubjectMatches(s, CallSignalsSubject));

        // 2) 不进入 PostgreSQL：realtime schema 中不存在任何 call 相关持久化表。
        await using var pg = new NpgsqlConnection(_fixture.PostgresConnectionString);
        await pg.OpenAsync(ct).ConfigureAwait(false);
        await using var cmd = new NpgsqlCommand(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = 'realtime'
              AND (table_name ILIKE '%call%' OR table_name ILIKE '%signal%');
            """,
            pg);
        await using var reader = await cmd.ExecuteReaderAsync(ct).ConfigureAwait(false);
        var tables = new List<string>();
        while (await reader.ReadAsync(ct).ConfigureAwait(false))
            tables.Add(reader.GetString(0));
        Assert.Empty(tables);
    }

    private static bool SubjectMatches(string pattern, string subject)
    {
        if (string.Equals(pattern, subject, StringComparison.Ordinal))
            return true;
        // 支持尾部通配符（例如 chat.call-signals.>）。
        if (pattern.EndsWith(">", StringComparison.Ordinal))
        {
            var prefix = pattern[..^1];
            return subject.StartsWith(prefix, StringComparison.Ordinal);
        }
        return false;
    }

    /// <summary>
    /// 临时信令路径探测器：订阅 Core NATS 的 chat.call-signals，收集对端需收到的 SDP 信令。
    /// </summary>
    private sealed class CallSignalProbe : IAsyncDisposable
    {
        private readonly string _url;
        private readonly Channel<CallSignalEnvelope> _signals = Channel.CreateUnbounded<CallSignalEnvelope>();
        private NatsConnection? _connection;
        private readonly object _lock = new();
        private readonly List<CallSignalEnvelope> _buffer = new();

        public CallSignalProbe(string url) => _url = url;

        public async Task StartAsync(CancellationToken ct)
        {
            _connection = new NatsConnection(new NatsOpts
            {
                Url = _url,
                Name = "chatapp-e2e-call-signal-probe"
            });
            await _connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);

            _ = Task.Run(async () =>
            {
                try
                {
                    await foreach (var msg in _connection.SubscribeAsync<string>(
                                       CallSignalsSubject,
                                       cancellationToken: ct)
                                       .ConfigureAwait(false))
                    {
                        if (string.IsNullOrWhiteSpace(msg.Data))
                            continue;
                        var signal = JsonSerializer.Deserialize(
                            msg.Data,
                            RealtimeJsonSerializerContext.Default.CallSignalEnvelope);
                        if (signal is not null)
                            _signals.Writer.TryWrite(signal);
                    }
                }
                catch (OperationCanceledException) { }
            }, ct);
        }

        public IReadOnlyList<CallSignalEnvelope> TakeAll()
        {
            var list = new List<CallSignalEnvelope>();
            while (_signals.Reader.TryRead(out var s))
                list.Add(s);
            lock (_lock)
            {
                list.AddRange(_buffer);
                _buffer.Clear();
            }
            return list;
        }

        public async ValueTask DisposeAsync()
        {
            if (_connection is not null)
                await _connection.DisposeAsync().ConfigureAwait(false);
        }
    }
}
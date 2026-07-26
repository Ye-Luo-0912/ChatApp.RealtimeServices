using System.Collections.Concurrent;
using ChatApp.Realtime.Abstractions.Routing;
using ChatApp.Realtime.Integration.Configuration;
using Microsoft.Extensions.Logging;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ChatApp.Realtime.Integration.JetStream;

/// <summary>
/// JetStream 流拓扑管理与 JetStream 持久化发布（带重连重试）。
/// </summary>
internal sealed class JetStreamTopologyManager
{
    private readonly NatsConnectionProvider _connectionProvider;
    private readonly RealtimeIntegrationOptions _options;
    private readonly ILogger _logger;
    private readonly ConcurrentDictionary<string, Task<INatsJSStream>> _streams = new(StringComparer.Ordinal);

    public JetStreamTopologyManager(
        NatsConnectionProvider connectionProvider,
        RealtimeIntegrationOptions options,
        ILogger logger)
    {
        _connectionProvider = connectionProvider;
        _options = options;
        _logger = logger;
    }

    /// <summary>
    /// 当前实例的 JetStream 上下文（用于不需要重连重试的一次性发布）。
    /// </summary>
    public INatsJSContext Context => _connectionProvider.Context;

    /// <summary>
    /// 是否启用按 Gateway 分片投递 Realtime Event。
    /// </summary>
    private bool IsShardedRoutingEnabled =>
        _options.RoutingMode is EventRoutingMode.Sharded
        && ShardedSubjectFormatter.IsSharded(_options.RealtimeEventsShardSubjectPattern);

    /// <summary>
    /// Realtime Event 分片通配符 subject（用于 JetStream 流配置）。
    /// </summary>
    private string RealtimeEventsShardWildcard =>
        ShardedSubjectFormatter.ToWildcard(_options.RealtimeEventsShardSubjectPattern);

    /// <summary>
    /// 确保指定流存在（带并发去重与失败重试）。
    /// </summary>
    public async Task<INatsJSStream> EnsureStreamAsync(
        string streamName,
        string subject,
        int maxAgeHours,
        CancellationToken ct)
    {
        while (true)
        {
            var task = _streams.GetOrAdd(streamName, _ => CreateOrUpdateStreamAsync(
                streamName,
                subject,
                maxAgeHours));
            try
            {
                return await task.WaitAsync(ct).ConfigureAwait(false);
            }
            catch
            {
                _streams.TryRemove(new KeyValuePair<string, Task<INatsJSStream>>(streamName, task));
                throw;
            }
        }
    }

    /// <summary>
    /// 通过 JetStream 持久化发布消息；遇到 <see cref="NatsJSPublishNoResponseException"/>
    /// 时按指数退避重试，并复用相同 MsgId 实现服务端去重。
    /// </summary>
    public async Task PublishJetStreamWithReconnectRetryAsync(
        string subject,
        string payload,
        string messageId,
        NatsHeaders? headers,
        CancellationToken ct)
    {
        var attempt = 0;
        while (true)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                await _connectionProvider.Context.PublishAsync(
                    subject,
                    payload,
                    opts: CreatePublishOptions(messageId),
                    headers: headers,
                    cancellationToken: ct).ConfigureAwait(false);
                return;
            }
            catch (NatsJSPublishNoResponseException ex) when (!ct.IsCancellationRequested)
            {
                attempt++;
                var delay = TimeSpan.FromMilliseconds(
                    Math.Min(2_000, 250 * Math.Pow(2, Math.Min(attempt - 1, 3))) +
                    Random.Shared.Next(0, 250));
                _logger.LogWarning(
                    ex,
                    "JetStream 发布未收到响应，将使用相同 MsgId 重试。Subject={Subject}；尝试={Attempt}；延迟={Delay}",
                    subject,
                    attempt,
                    delay);
                await Task.Delay(delay, ct).ConfigureAwait(false);
            }
        }
    }

    private async Task<INatsJSStream> CreateOrUpdateStreamAsync(
        string streamName,
        string subject,
        int maxAgeHours)
    {
        if (!_options.ManageStreams)
        {
            return await _connectionProvider.Context.GetStreamAsync(
                streamName,
                new StreamInfoRequest(),
                CancellationToken.None).ConfigureAwait(false);
        }

        var subjects = streamName.Equals(_options.RealtimeEventsStream, StringComparison.Ordinal)
            ? BuildRealtimeEventsStreamSubjects()
            : new[] { subject };
        var config = new StreamConfig(streamName, subjects)
        {
            Storage = StreamConfigStorage.File,
            Retention = StreamConfigRetention.Limits,
            Discard = StreamConfigDiscard.Old,
            DuplicateWindow = TimeSpan.FromMinutes(_options.DuplicateWindowMinutes),
            MaxAge = TimeSpan.FromHours(maxAgeHours),
            MaxBytes = _options.MaxBytes,
            MaxMsgSize = _options.MaxMessageSize,
            NumReplicas = _options.Replicas
        };
        return await _connectionProvider.Context.CreateOrUpdateStreamAsync(config, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// 构建 REALTIME_EVENTS 流的 subject 列表。
    /// <para>
    /// 分片模式下额外包含通配符 subject（<c>chat.realtime-events.&gt;</c>），
    /// 使流能够接收所有 Gateway 实例的分片投递。
    /// </para>
    /// </summary>
    private string[] BuildRealtimeEventsStreamSubjects()
    {
        var list = new List<string>(3)
        {
            _options.RealtimeEventsSubject,
            _options.AccountCleanupSubject
        };

        if (IsShardedRoutingEnabled
            && ShardedSubjectFormatter.IsSharded(_options.RealtimeEventsShardSubjectPattern))
        {
            list.Add(RealtimeEventsShardWildcard);
        }

        return list.ToArray();
    }

    private static NatsJSPubOpts CreatePublishOptions(string messageId) => new()
    {
        MsgId = messageId,
        RetryAttempts = 3,
        RetryWaitBetweenAttempts = TimeSpan.FromMilliseconds(200)
    };

    /// <summary>
    /// 构造 JetStream 发布选项（按 MsgId 服务端去重）。
    /// </summary>
    public static NatsJSPubOpts BuildPublishOptions(string messageId) => CreatePublishOptions(messageId);
}

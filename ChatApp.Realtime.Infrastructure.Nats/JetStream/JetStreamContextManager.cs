using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Nats.Configuration;
using ChatApp.Realtime.Infrastructure.Nats.Queueing;
using Microsoft.Extensions.Logging;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;
using NATS.Net;

namespace ChatApp.Realtime.Infrastructure.Nats.JetStream;

public sealed class JetStreamContextManager
{
    private readonly NatsConnectionClient _connectionClient;
    private readonly RealtimeQueueOptions _queueOptions;
    private readonly JetStreamStreamOptions _streams;
    private readonly ILogger<JetStreamContextManager> _logger;

    private readonly Lazy<INatsJSContext> _context;
    private readonly Lazy<INatsJSStream> _incomingMessagesStream;
    private readonly Lazy<INatsJSStream> _realtimeEventsStream;

    public JetStreamContextManager(
        NatsConnectionClient connectionClient,
        RealtimeQueueOptions queueOptions,
        JetStreamStreamOptions streams,
        ILogger<JetStreamContextManager> logger)
    {
        _connectionClient = connectionClient;
        _queueOptions = queueOptions;
        _streams = streams;
        _logger = logger;

        _context = new Lazy<INatsJSContext>(CreateContext);
        _incomingMessagesStream = new Lazy<INatsJSStream>(() => EnsureStreamAsync(
            _streams.IncomingMessages, _queueOptions.Topics.IncomingMessages).GetAwaiter().GetResult());
        _realtimeEventsStream = new Lazy<INatsJSStream>(() => EnsureStreamAsync(
            _streams.RealtimeEvents, _queueOptions.Topics.RealtimeEvents).GetAwaiter().GetResult());
    }

    public INatsJSContext Context => _context.Value;

    public INatsJSStream IncomingMessagesStream => _incomingMessagesStream.Value;
    public INatsJSStream RealtimeEventsStream => _realtimeEventsStream.Value;

    public async Task<INatsJSConsumer> GetOrCreateIncomingMessagesConsumerAsync(CancellationToken ct = default)
    {
        return await GetOrCreateConsumerAsync(
            IncomingMessagesStream,
            _queueOptions.ConsumerGroup,
            _queueOptions.Topics.IncomingMessages,
            ct).ConfigureAwait(false);
    }

    public async Task<INatsJSConsumer> GetOrCreateRealtimeEventsConsumerAsync(CancellationToken ct = default)
    {
        return await GetOrCreateConsumerAsync(
            RealtimeEventsStream,
            _queueOptions.ConsumerGroup,
            _queueOptions.Topics.RealtimeEvents,
            ct).ConfigureAwait(false);
    }

    private INatsJSContext CreateContext()
    {
        _logger.LogInformation("正在创建 JetStream 上下文。");
        return _connectionClient.Client.CreateJetStreamContext();
    }

    private async Task<INatsJSStream> EnsureStreamAsync(string streamName, string subject, CancellationToken ct = default)
    {
        var js = Context;

        try
        {
            var stream = await js.GetStreamAsync(streamName, cancellationToken: ct).ConfigureAwait(false);
            _logger.LogInformation("JetStream 流已存在。流名={Stream}", streamName);
            return stream;
        }
        catch (NatsJSException)
        {
            _logger.LogInformation("正在创建 JetStream 流。流名={Stream}；Subject={Subject}", streamName, subject);

            var config = new StreamConfig(streamName, new[] { subject })
            {
                Storage = StreamConfigStorage.File,
                Retention = StreamConfigRetention.Limits,
                DuplicateWindow = TimeSpan.FromMinutes(2)
            };

            return await js.CreateStreamAsync(config, ct).ConfigureAwait(false);
        }
    }

    private static async Task<INatsJSConsumer> GetOrCreateConsumerAsync(
        INatsJSStream stream,
        string consumerName,
        string filterSubject,
        CancellationToken ct)
    {
        try
        {
            return await stream.GetConsumerAsync(consumerName, ct).ConfigureAwait(false);
        }
        catch (NatsJSException)
        {
            var config = new ConsumerConfig(consumerName)
            {
                FilterSubject = filterSubject,
                AckPolicy = ConsumerConfigAckPolicy.Explicit,
                AckWait = TimeSpan.FromSeconds(30),
                MaxDeliver = 10,
                DeliverPolicy = ConsumerConfigDeliverPolicy.All
            };

            return await stream.CreateOrUpdateConsumerAsync(config, ct).ConfigureAwait(false);
        }
    }
}

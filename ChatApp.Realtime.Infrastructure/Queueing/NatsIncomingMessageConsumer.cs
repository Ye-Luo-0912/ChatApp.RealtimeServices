using System.Runtime.CompilerServices;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Queueing;
using ChatApp.Realtime.Infrastructure.Serialization;
using Microsoft.Extensions.Logging;

namespace ChatApp.Realtime.Infrastructure.Queueing;

/// <summary>
/// 基于 Core NATS 的入站消息消费者。
/// Core NATS 不提供持久化和确认语义，适合当前本地开发和轻量链路验证；生产可靠投递后续应升级到 JetStream。
/// </summary>
public sealed class NatsIncomingMessageConsumer : IIncomingMessageConsumer
{
    private readonly RealtimeQueueOptions _options;
    private readonly NatsConnectionClient _connectionClient;
    private readonly ILogger<NatsIncomingMessageConsumer> _logger;

    public NatsIncomingMessageConsumer(
        RealtimeQueueOptions options,
        NatsConnectionClient connectionClient,
        ILogger<NatsIncomingMessageConsumer> logger)
    {
        _options = options;
        _connectionClient = connectionClient;
        _logger = logger;
    }

    /// <summary>
    /// 从NATS队列中异步消费入站消息。
    /// </summary>
    /// <param name="ct">用于取消操作的取消令牌。</param>
    /// <returns>返回一个可枚举的异步序列，包含反序列化后的<see cref="IncomingMessageCommand"/>对象。</returns>
    public async IAsyncEnumerable<IncomingMessageCommand> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken ct = default)
    {
        _logger.LogInformation(
            "NATS 入站消息消费者已订阅。Subject={Subject}；队列组={QueueGroup}",
            _options.Topics.IncomingMessages,
            _options.ConsumerGroup);

        await foreach (var msg in _connectionClient.Client.SubscribeAsync<string>(
                           _options.Topics.IncomingMessages,
                           _options.ConsumerGroup,
                           cancellationToken: ct))
        {
            IncomingMessageCommand? command = null;

            try
            {
                msg.EnsureSuccess();

                if (string.IsNullOrWhiteSpace(msg.Data))
                {
                    _logger.LogWarning("NATS 入站消息为空，已跳过。Subject={Subject}", msg.Subject);
                    continue;
                }

                command = JsonSerializer.Deserialize(
                    msg.Data,
                    RealtimeJsonSerializerContext.Default.IncomingMessageCommand);
            }
            catch (JsonException ex)
            {
                _logger.LogError(ex, "NATS 入站消息反序列化失败。Subject={Subject}", msg.Subject);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "NATS 入站消息读取失败。Subject={Subject}", msg.Subject);
            }

            if (command is not null)
            {
                yield return command;
            }
        }
    }
}

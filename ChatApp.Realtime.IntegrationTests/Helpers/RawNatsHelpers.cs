using System.Text;
using System.Text.Json;
using ChatApp.Realtime.Abstractions.Auth;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Integration.Serialization;
using NATS.Client.Core;
using NATS.Client.JetStream;
using NATS.Client.JetStream.Models;

namespace ChatApp.Realtime.IntegrationTests.Helpers;

/// <summary>
/// Low-level NATS helpers for forging identity headers / omitting them (Gateway stand-in bypass).
/// </summary>
internal static class RawNatsHelpers
{
    public static async Task PublishIncomingWithoutIdentityAsync(
        string natsUrl,
        IncomingMessageCommand command,
        CancellationToken ct)
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = natsUrl,
            Name = "chatapp-e2e-raw-pub"
        });
        await connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);
        var js = new NatsJSContext(connection);
        await js.PublishAsync(
                "chat.incoming-messages",
                RealtimeWireSerializer.Serialize(command),
                opts: new NatsJSPubOpts { MsgId = command.CommandId },
                cancellationToken: ct)
            .ConfigureAwait(false);
    }

    public static async Task<SyncBootstrapPage> QuerySyncBootstrapWithTrustedUserAsync(
        string natsUrl,
        SyncBootstrapQuery query,
        long trustedUserId,
        CancellationToken ct)
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = natsUrl,
            Name = "chatapp-e2e-raw-req",
            RequestTimeout = TimeSpan.FromSeconds(15)
        });
        await connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);

        var headers = new NatsHeaders
        {
            [RealtimeIdentityHeaders.UserId] = trustedUserId.ToString()
        };

        var response = await connection.RequestAsync<string, string>(
                "chat.sync.bootstrap",
                RealtimeWireSerializer.Serialize(query),
                headers: headers,
                cancellationToken: ct)
            .ConfigureAwait(false);
        response.EnsureSuccess();

        return RealtimeWireSerializer.DeserializeSyncBootstrapPage(response.Data!)
               ?? throw new JsonException("sync bootstrap response could not be deserialized.");
    }

    public static async Task<DeadLetterMessage> WaitForDeadLetterAsync(
        string natsUrl,
        string? commandId,
        string? reasonCode,
        CancellationToken ct)
    {
        await using var connection = new NatsConnection(new NatsOpts
        {
            Url = natsUrl,
            Name = "chatapp-e2e-dl"
        });
        await connection.ConnectAsync().AsTask().WaitAsync(ct).ConfigureAwait(false);
        var js = new NatsJSContext(connection);
        var stream = await js.GetStreamAsync("DEAD_LETTERS", cancellationToken: ct)
            .ConfigureAwait(false);
        var consumer = await stream.CreateOrUpdateConsumerAsync(
                new ConsumerConfig
                {
                    Name = $"e2e-dl-{Guid.NewGuid():N}"[..28],
                    AckPolicy = ConsumerConfigAckPolicy.Explicit,
                    FilterSubjects = ["chat.dead-letters"],
                    DeliverPolicy = ConsumerConfigDeliverPolicy.New,
                    InactiveThreshold = TimeSpan.FromMinutes(1)
                },
                cancellationToken: ct)
            .ConfigureAwait(false);

        await foreach (var msg in consumer.ConsumeAsync<string>(cancellationToken: ct)
                           .ConfigureAwait(false))
        {
            if (string.IsNullOrWhiteSpace(msg.Data))
            {
                await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
                continue;
            }

            var letter = JsonSerializer.Deserialize<DeadLetterMessage>(msg.Data);
            await msg.AckAsync(cancellationToken: ct).ConfigureAwait(false);
            if (letter is null)
                continue;

            if (commandId is not null
                && !string.Equals(letter.CommandId, commandId, StringComparison.Ordinal))
            {
                continue;
            }

            if (reasonCode is not null
                && !string.Equals(letter.ReasonCode, reasonCode, StringComparison.Ordinal))
            {
                continue;
            }

            return letter;
        }

        throw new InvalidOperationException("Timed out waiting for dead letter.");
    }
}

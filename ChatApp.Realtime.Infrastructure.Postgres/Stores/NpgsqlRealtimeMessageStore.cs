using ChatApp.Realtime.Abstractions.Stores;
using ChatApp.Realtime.Infrastructure.Postgres.Clients;
using ChatApp.Realtime.Infrastructure.Postgres.Data;
using Microsoft.Extensions.Logging;
using Npgsql;

namespace ChatApp.Realtime.Infrastructure.Postgres.Stores;

public sealed class NpgsqlRealtimeMessageStore : IRealtimeMessageStore
{
    private readonly RealtimeDatabaseClient _databaseClient;
    private readonly RealtimeDatabaseSchema _databaseSchema;
    private readonly ILogger<NpgsqlRealtimeMessageStore> _logger;

    public NpgsqlRealtimeMessageStore(
        RealtimeDatabaseClient databaseClient,
        RealtimeDatabaseSchema databaseSchema,
        ILogger<NpgsqlRealtimeMessageStore> logger)
    {
        _databaseClient = databaseClient;
        _databaseSchema = databaseSchema;
        _logger = logger;
    }

    public async Task<bool> SaveAsync(RealtimeMessageRecord message, CancellationToken ct = default)
    {
        await using var connection = await _databaseClient
            .GetDataSource()
            .OpenConnectionAsync(ct)
            .ConfigureAwait(false);

        await using var command = new NpgsqlCommand(
            $"""
            INSERT INTO {_databaseSchema.MessagesTableSql} (
                message_id,
                client_message_id,
                sender_user_id,
                sender_session_id,
                receiver_user_id,
                content,
                received_at_ms,
                created_at_ms
            )
            VALUES (
                @message_id,
                @client_message_id,
                @sender_user_id,
                @sender_session_id,
                @receiver_user_id,
                @content,
                @received_at_ms,
                @created_at_ms
            )
            ON CONFLICT (sender_user_id, client_message_id) DO NOTHING;
            """,
            connection);

        command.Parameters.AddWithValue("message_id", message.MessageId);
        command.Parameters.AddWithValue("client_message_id", message.ClientMessageId);
        command.Parameters.AddWithValue("sender_user_id", message.SenderUserId);
        command.Parameters.AddWithValue("sender_session_id", message.SenderSessionId);
        command.Parameters.AddWithValue("receiver_user_id", message.ReceiverUserId);
        command.Parameters.AddWithValue("content", message.Content);
        command.Parameters.AddWithValue("received_at_ms", message.ReceivedAtMs);
        command.Parameters.AddWithValue("created_at_ms", DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());

        var affectedRows = await command.ExecuteNonQueryAsync(ct).ConfigureAwait(false);

        if (affectedRows > 0)
        {
            _logger.LogInformation(
                "实时消息已通过 Npgsql 写入数据库。消息编号={MessageId}；发送用户={SenderUserId}；接收用户={ReceiverUserId}",
                message.MessageId,
                message.SenderUserId,
                message.ReceiverUserId);
            return true;
        }

        _logger.LogInformation(
            "实时消息已存在，跳过重复写入。客户端消息编号={ClientMessageId}；发送用户={SenderUserId}",
            message.ClientMessageId,
            message.SenderUserId);

        return false;
    }
}

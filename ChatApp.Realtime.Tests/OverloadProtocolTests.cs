using System.Text.Json;
using ChatApp.Realtime.Abstractions.Conversations;
using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Abstractions.Messaging.History;
using ChatApp.Realtime.Abstractions.Protocol;
using ChatApp.Realtime.Abstractions.Sync;
using ChatApp.Realtime.Infrastructure.Core.Serialization;

namespace ChatApp.Realtime.Tests;

public sealed class OverloadProtocolTests
{
    private const int RetryAfterMs = 500;
    private const string QueueKind = "history_query";

    [Fact]
    public void MessageHistoryPage_ServerBusy_SetsOverloadFields()
    {
        var page = MessageHistoryPage.ServerBusy("req-1", RetryAfterMs, QueueKind);
        Assert.False(page.Succeeded);
        Assert.Equal("server_busy", page.ErrorCode);
        Assert.Equal(RetryAfterMs, page.RetryAfterMs);
        Assert.Equal(QueueKind, page.QueueKind);
    }

    [Fact]
    public void SyncBootstrapPage_ServerBusy_SetsOverloadFields()
    {
        var page = SyncBootstrapPage.ServerBusy("req-2", RetryAfterMs, "sync_bootstrap");
        Assert.False(page.Succeeded);
        Assert.Equal("server_busy", page.ErrorCode);
        Assert.Equal(RetryAfterMs, page.RetryAfterMs);
        Assert.Equal("sync_bootstrap", page.QueueKind);
    }

    [Fact]
    public void ConversationListPage_ServerBusy_SetsOverloadFields()
    {
        var page = ConversationListPage.ServerBusy("req-3", RetryAfterMs, "conversation_list");
        Assert.False(page.Succeeded);
        Assert.Equal("server_busy", page.ErrorCode);
        Assert.Equal(RetryAfterMs, page.RetryAfterMs);
        Assert.Equal("conversation_list", page.QueueKind);
    }

    [Fact]
    public void ConversationMarkReadResult_ServerBusy_SetsOverloadFields()
    {
        var result = ConversationMarkReadResult.ServerBusy("req-4", RetryAfterMs, "conversation_mark_read");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("conversation_mark_read", result.QueueKind);
    }

    [Fact]
    public void ConversationSetPrefsResult_ServerBusy_SetsOverloadFields()
    {
        var result = ConversationSetPrefsResult.ServerBusy("req-5", RetryAfterMs, "conversation_set_prefs");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("conversation_set_prefs", result.QueueKind);
    }

    [Fact]
    public void GroupConversationResult_ServerBusy_SetsOverloadFields()
    {
        var result = GroupConversationResult.ServerBusy("req-6", RetryAfterMs, "group_conversation");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("group_conversation", result.QueueKind);
    }

    [Fact]
    public void MessageEditResult_ServerBusy_SetsOverloadFields()
    {
        var result = MessageEditResult.ServerBusy("req-7", RetryAfterMs, "message_edit");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("message_edit", result.QueueKind);
    }

    [Fact]
    public void MessageRecallResult_ServerBusy_SetsOverloadFields()
    {
        var result = MessageRecallResult.ServerBusy("req-8", RetryAfterMs, "message_recall");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("message_recall", result.QueueKind);
    }

    [Fact]
    public void MessageReactionResult_ServerBusy_SetsOverloadFields()
    {
        var result = MessageReactionResult.ServerBusy("req-9", RetryAfterMs, "message_reaction");
        Assert.False(result.Succeeded);
        Assert.Equal("server_busy", result.ErrorCode);
        Assert.Equal(RetryAfterMs, result.RetryAfterMs);
        Assert.Equal("message_reaction", result.QueueKind);
    }

    [Fact]
    public void ServerBusy_JsonRoundTrip_PreservesOverloadFields()
    {
        var original = MessageHistoryPage.ServerBusy("req-rt", 750, "history_query");
        var json = JsonSerializer.Serialize(original, RealtimeJsonSerializerContext.Default.MessageHistoryPage);
        var deserialized = JsonSerializer.Deserialize(json, RealtimeJsonSerializerContext.Default.MessageHistoryPage);

        Assert.NotNull(deserialized);
        Assert.False(deserialized!.Succeeded);
        Assert.Equal("server_busy", deserialized.ErrorCode);
        Assert.Equal(750, deserialized.RetryAfterMs);
        Assert.Equal("history_query", deserialized.QueueKind);
        Assert.Equal("req-rt", deserialized.RequestId);
    }

    [Fact]
    public void SuccessResult_OverloadFieldsAreNull_InJson()
    {
        var original = MessageHistoryPage.Success("req-ok", [], null, false);
        var json = JsonSerializer.Serialize(original, RealtimeJsonSerializerContext.Default.MessageHistoryPage);

        Assert.DoesNotContain("retryAfterMs", json);
        Assert.DoesNotContain("queueKind", json);
    }

    [Fact]
    public void RealtimeServerBusyException_CarriesRetryAndQueueKind()
    {
        var ex = new RealtimeServerBusyException(500, "sync_bootstrap");
        Assert.Equal(500, ex.RetryAfterMs);
        Assert.Equal("sync_bootstrap", ex.QueueKind);
        Assert.Contains("queue_kind=sync_bootstrap", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void FailedResult_DoesNotSetOverloadFields()
    {
        var page = MessageHistoryPage.Failed("req-fail", "not_found", "消息不存在");
        Assert.Null(page.RetryAfterMs);
        Assert.Null(page.QueueKind);
        Assert.Equal("not_found", page.ErrorCode);
    }
}

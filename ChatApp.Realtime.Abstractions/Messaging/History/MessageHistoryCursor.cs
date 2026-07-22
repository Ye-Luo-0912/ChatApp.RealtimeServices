namespace ChatApp.Realtime.Abstractions.Messaging.History;

public sealed record MessageHistoryCursor(long ReceivedAtMs, string MessageId);

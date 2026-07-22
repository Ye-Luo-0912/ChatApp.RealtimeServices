namespace ChatApp.Realtime.Abstractions.Stores;

public sealed record RealtimeMessagePersistResult(bool IsNew, string MessageId);

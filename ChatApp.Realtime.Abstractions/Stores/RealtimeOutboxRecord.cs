using ChatApp.Realtime.Abstractions.Events;

namespace ChatApp.Realtime.Abstractions.Stores;

public sealed record RealtimeOutboxRecord(
    string EventId,
    RealtimeEvent Event,
    int AttemptCount,
    string LockOwner,
    string ClaimToken,
    ReadOnlyMemory<byte>? PayloadUtf8 = null);

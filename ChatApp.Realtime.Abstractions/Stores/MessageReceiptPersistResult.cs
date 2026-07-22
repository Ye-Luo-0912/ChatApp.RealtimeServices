namespace ChatApp.Realtime.Abstractions.Stores;

public enum MessageReceiptPersistStatus : byte
{
    Applied = 1,
    Unchanged = 2,
    MessageNotFound = 3,
    ReceiverMismatch = 4
}

public sealed record MessageReceiptPersistResult(
    MessageReceiptPersistStatus Status,
    string MessageId,
    long? SenderUserId = null);
using ChatApp.Realtime.Abstractions.Events;
using ChatApp.Realtime.Abstractions.Messaging;

namespace ChatApp.Realtime.Abstractions.Messaging;

public interface IUserAccountDeletedProcessor
{
    Task<MessageProcessResult> ProcessAsync(RealtimeEvent evt, CancellationToken ct = default);
}

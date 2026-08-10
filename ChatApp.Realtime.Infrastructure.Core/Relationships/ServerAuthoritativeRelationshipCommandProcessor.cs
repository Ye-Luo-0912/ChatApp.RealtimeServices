using ChatApp.Realtime.Abstractions.Relationships;

namespace ChatApp.Realtime.Infrastructure.Core.Relationships;

/// <summary>
/// Fail-closed boundary while relationship writes are owned by ChatApp.Server.
/// TCP callers receive an explicit migration error instead of mutating the
/// legacy realtime relationship tables and creating a second source of truth.
/// </summary>
public sealed class ServerAuthoritativeRelationshipCommandProcessor : IRelationshipCommandProcessor
{
    public static ServerAuthoritativeRelationshipCommandProcessor Instance { get; } = new();

    private ServerAuthoritativeRelationshipCommandProcessor()
    {
    }

    public Task<RelationshipCommandResult> ProcessAsync(
        RelationshipCommand command,
        CancellationToken ct = default) =>
        Task.FromResult(RelationshipCommandResult.Failed(
            command.RequestId,
            "relationship_write_moved_to_server",
            "关系变更必须通过 ChatApp.Server HTTP API 提交。"));
}

using ChatApp.Realtime.Infrastructure.Postgres.Messaging;
using Microsoft.Extensions.Logging.Abstractions;

namespace ChatApp.Realtime.Tests.TestDoubles;

/// <summary>
/// 测试用 <see cref="IConversationMessageMutationPolicy"/> 单例工厂。
/// 避免在每个测试中重复构造 <see cref="PostgresConversationMessageMutationPolicy"/>。
/// </summary>
internal static class TestMutationPolicy
{
    public static IConversationMessageMutationPolicy Instance { get; } =
        new PostgresConversationMessageMutationPolicy(
            NullLogger<PostgresConversationMessageMutationPolicy>.Instance);
}

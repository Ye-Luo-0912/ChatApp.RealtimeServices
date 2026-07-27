namespace ChatApp.Realtime.Abstractions.Messaging.History;

/// <summary>
/// 历史分页游标。
/// <para>
/// Before 模式（历史翻页）使用 <see cref="ReceivedAtMs"/> + <see cref="MessageId"/>。
/// After 模式（增量追赶）使用 <see cref="ChangedAtMs"/> + <see cref="MessageId"/>，
/// 因为编辑、撤回和 Reaction 都会推进 changed_at_ms 变更水位。
/// </para>
/// <para>
/// P0-2：After 模式不再误用 ReceivedAtMs 作为水位，避免分页循环。
/// </para>
/// </summary>
public sealed record MessageHistoryCursor(long ReceivedAtMs, string MessageId, long? ChangedAtMs = null);

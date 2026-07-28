namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 群成员的一个入群/离群时间段。
/// <para>
/// <see cref="LeftAtMs"/> 为 null 表示当前仍在群中；
/// 非 null 表示已离群，<see cref="LeftReason"/> 描述离群原因（leave / removed / dissolved）。
/// </para>
/// <para>
/// 用于历史查询时过滤可见时间段：重新入群后不能查看缺席期间的消息。
/// </para>
/// </summary>
public sealed record MembershipPeriod
{
    /// <summary>入群时间戳（毫秒）。</summary>
    public required long JoinedAtMs { get; init; }

    /// <summary>离群时间戳（毫秒）。null 表示当前仍在群中。</summary>
    public long? LeftAtMs { get; init; }

    /// <summary>离群原因（leave / removed / dissolved）。仍在群中时为 null。</summary>
    public string? LeftReason { get; init; }
}

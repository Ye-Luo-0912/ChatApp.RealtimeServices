namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 网关目录查询结果分类。
/// <para>
/// P0-9：用于区分"用户离线"与"查询失败"两种空结果场景。
/// 前者（<see cref="UserOffline"/>）属于正常情况，发布方不应投递；
/// 后者（<see cref="LookupFailure"/> / <see cref="PartialLookupFailure"/>）
/// 需要枚举所有已知活跃 Gateway shards 分别发布，避免分片模式下广播 fallback 无人消费。
/// </para>
/// </summary>
public enum GatewayLookupResultKind
{
    /// <summary>
    /// 查询成功且用户在线（至少一个 Gateway 实例）。
    /// </summary>
    Success,

    /// <summary>
    /// 查询成功但用户离线（无在线 Gateway 实例）。发布方正常不投递。
    /// </summary>
    UserOffline,

    /// <summary>
    /// 查询失败（Redis 异常 / 超时等）。发布方应回退到所有活跃 shards 投递。
    /// </summary>
    LookupFailure,

    /// <summary>
    /// 批量查询部分失败。发布方应回退到所有活跃 shards 投递。
    /// </summary>
    PartialLookupFailure
}

/// <summary>
/// 单用户网关目录查询结果，包含查询状态与返回的 Gateway 实例 ID 集合。
/// </summary>
public sealed record GatewayLookupResult(
    GatewayLookupResultKind Kind,
    IReadOnlyList<string> Gateways);

/// <summary>
/// 批量用户网关目录查询结果，包含查询状态与用户 ID -> Gateway 实例 ID 集合的映射。
/// </summary>
public sealed record GatewayLookupManyResult(
    GatewayLookupResultKind Kind,
    IReadOnlyDictionary<long, IReadOnlyList<string>> GatewayMap);

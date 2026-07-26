using System.Globalization;

namespace ChatApp.Realtime.Abstractions.Routing;

/// <summary>
/// 分片 subject 格式化工具：将实例 ID 填入 subject 模板。
/// <para>
/// 模板中使用 <c>{0}</c> 作为实例 ID 占位符，如
/// <c>chat.realtime-events.{0}</c> -> <c>chat.realtime-events.gateway-01</c>。
/// </para>
/// </summary>
public static class ShardedSubjectFormatter
{
    /// <summary>
    /// 将实例 ID 填入 subject 模板。
    /// </summary>
    /// <param name="pattern">包含 <c>{0}</c> 占位符的 subject 模板。</param>
    /// <param name="instanceId">Gateway 实例 ID。</param>
    /// <returns>填入实例 ID 后的 subject。</returns>
    public static string Format(string pattern, string instanceId)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        ArgumentException.ThrowIfNullOrWhiteSpace(instanceId);
        return string.Format(CultureInfo.InvariantCulture, pattern, instanceId);
    }

    /// <summary>
    /// 判断 subject 模板是否包含分片占位符。
    /// </summary>
    /// <param name="pattern">待检查的 subject 模板。</param>
    /// <returns>包含 <c>{0}</c> 时返回 true。</returns>
    public static bool IsSharded(string pattern)
    {
        return !string.IsNullOrWhiteSpace(pattern)
            && pattern.Contains("{0}", StringComparison.Ordinal);
    }

    /// <summary>
    /// 将分片 subject 模板转换为通配符 subject，用于 JetStream 流配置。
    /// <para>
    /// 例如 <c>chat.realtime-events.{0}</c> -> <c>chat.realtime-events.&gt;</c>。
    /// </para>
    /// </summary>
    /// <param name="pattern">包含 <c>{0}</c> 占位符的 subject 模板。</param>
    /// <returns>将 <c>{0}</c> 替换为 <c>&gt;</c> 后的通配符 subject；模板不含占位符时返回原值。</returns>
    public static string ToWildcard(string pattern)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(pattern);
        return pattern.Contains("{0}", StringComparison.Ordinal)
            ? pattern.Replace("{0}", ">", StringComparison.Ordinal)
            : pattern;
    }
}

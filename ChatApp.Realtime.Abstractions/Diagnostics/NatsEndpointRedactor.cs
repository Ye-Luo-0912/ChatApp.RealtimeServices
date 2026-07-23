using System.Text;

namespace ChatApp.Realtime.Abstractions.Diagnostics;

/// <summary>
/// 日志用 NATS 连接信息脱敏，避免把 userinfo/密码写入日志。
/// </summary>
public static class NatsEndpointRedactor
{
    public static string ForLog(string? endpoint)
    {
        if (string.IsNullOrWhiteSpace(endpoint))
            return "(unset)";

        if (!Uri.TryCreate(endpoint.Trim(), UriKind.Absolute, out var uri))
            return "(invalid)";

        var builder = new StringBuilder();
        builder.Append(uri.Scheme).Append("://");
        if (!string.IsNullOrEmpty(uri.UserInfo))
            builder.Append("***@");
        builder.Append(uri.Host);
        if (!uri.IsDefaultPort)
            builder.Append(':').Append(uri.Port);
        return builder.ToString();
    }
}

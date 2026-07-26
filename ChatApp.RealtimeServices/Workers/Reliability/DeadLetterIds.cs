using System.Security.Cryptography;
using System.Text;

namespace ChatApp.RealtimeServices.Workers.Reliability;

/// <summary>
/// 提取自 IncomingMessageWorker / MessageReceiptWorker 的死信 ID 生成逻辑。
/// 以 "commandId:reasonCode" 的 UTF-8 字节做 SHA-256，输出小写十六进制字符串。
/// </summary>
internal static class DeadLetterIds
{
    public static string Create(string commandId, string reasonCode)
    {
        var bytes = Encoding.UTF8.GetBytes($"{commandId}:{reasonCode}");
        return Convert.ToHexStringLower(SHA256.HashData(bytes));
    }
}

namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 二-4：隐私设置接口。
/// <para>
/// 用于查询用户的隐私偏好（如谁可以发起单聊、谁可以查看在线状态等）。
/// 当前 realtime schema 无独立 privacy_settings 表，通过本接口抽象。
/// </para>
/// <para>
/// 默认 Noop 实现返回"开放"（不阻塞），待外部系统接入时替换。
/// </para>
/// </summary>
public interface IPrivacySettingStore
{
    /// <summary>
    /// 查询指定用户是否允许来自 targetUserId 的单聊消息。
    /// </summary>
    /// <param name="userId">隐私设置所有者。</param>
    /// <param name="targetUserId">请求单聊的用户。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>允许时返回 true。</returns>
    Task<bool> AllowsDirectMessageAsync(
        long userId,
        long targetUserId,
        CancellationToken cancellationToken = default);
}
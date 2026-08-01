namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 二-2：用户屏蔽列表接口。
/// <para>
/// 用于校验发送方是否被接收方屏蔽。当前 realtime schema 无独立 block_list 表，
/// 通过本接口抽象，由外部系统（如社交关系服务）提供实现。
/// </para>
/// <para>
/// 默认 Noop 实现返回"未屏蔽"（不阻塞），待外部系统接入时替换。
/// </para>
/// </summary>
public interface IBlockListStore
{
    /// <summary>
    /// 检查 sender 是否被 receiver 屏蔽。
    /// </summary>
    /// <param name="receiverUserId">接收方用户编号（屏蔽操作的主体）。</param>
    /// <param name="senderUserId">发送方用户编号（被屏蔽方）。</param>
    /// <param name="cancellationToken">取消令牌。</param>
    /// <returns>被屏蔽时返回 true。</returns>
    Task<bool> IsBlockedAsync(
        long receiverUserId,
        long senderUserId,
        CancellationToken cancellationToken = default);
}

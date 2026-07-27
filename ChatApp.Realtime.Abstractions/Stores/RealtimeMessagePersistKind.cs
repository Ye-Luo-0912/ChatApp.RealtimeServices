namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 消息幂等写入结果：新建、同指纹重放、或同键不同内容冲突。
/// </summary>
public enum RealtimeMessagePersistKind : byte
{
    Created = 0,
    Duplicate = 1,
    ContentConflict = 2,

    /// <summary>
    /// 附件绑定失败（缺失 / 非 Confirmed / 非上传者拥有）。事务已回滚。
    /// </summary>
    AttachmentBindFailed = 3,

    /// <summary>
    /// 发送方不是会话成员等权限拒绝。事务已回滚。
    /// </summary>
    NotAllowed = 4,

    /// <summary>
    /// P0-2：发送方或接收方用户已注销（Deleting / Deleted）。事务已回滚。
    /// </summary>
    UserDeleted = 5
}

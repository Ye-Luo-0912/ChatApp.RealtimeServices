namespace ChatApp.Realtime.Abstractions.Attachments;

/// <summary>
/// 对象存储抽象。P1-3 附件闭环依赖：HEAD 校验对象真实元数据、删除对象、
/// 生成短期签名下载 URL。具体实现（S3 / 本地 / 兼容 MinIO）由宿主注入。
/// </summary>
public interface IObjectStorage
{
    /// <summary>
    /// 探测对象元数据（HEAD）。在 <see cref="AttachmentScanProcessor"/> 中用于校验
    /// 实际 Size / Hash / Content-Type 与票证一致。对象不存在时返回 null。
    /// </summary>
    Task<ObjectHead?> HeadAsync(string objectKey, CancellationToken ct = default);

    /// <summary>删除对象（幂等；对象不存在视为成功）。</summary>
    Task DeleteAsync(string objectKey, CancellationToken ct = default);

    /// <summary>
    /// 生成短期签名下载 URL（有效期由 <paramref name="ttl"/> 决定）。
    /// 用于「下载授权」：仅当附件状态为 Available/Bound 时签发。
    /// </summary>
    Task<string> CreateSignedDownloadUrlAsync(
        string objectKey,
        TimeSpan ttl,
        CancellationToken ct = default);
}
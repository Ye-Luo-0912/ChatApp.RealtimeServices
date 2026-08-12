namespace ChatApp.Realtime.Abstractions.Stores;

/// <summary>
/// 附件绑定结果（VOICE-MSG-1）：携带成功绑定的记录与每个失败附件对应的稳定错误码。
/// <para>
/// 发送前必须再次确认附件为 <see cref="AttachmentStatus.Available"/>（或 legacy
/// <see cref="AttachmentStatus.Confirmed"/>）且归属合法；任一附件不可绑定即整体失败
///（<see cref="Success"/> = false），绝不绑定子集、绝不静默跳过。
/// </para>
/// </summary>
public sealed record AttachmentBindResult(
    bool Success,
    IReadOnlyList<RealtimeAttachmentRecord> BoundRecords,
    IReadOnlyDictionary<string, AttachmentBindErrorCode> Errors)
{
    /// <summary>全部附件成功绑定。</summary>
    public static AttachmentBindResult Ok(IReadOnlyList<RealtimeAttachmentRecord> boundRecords) =>
        new(true, boundRecords, new Dictionary<string, AttachmentBindErrorCode>());

    /// <summary>存在不可绑定附件，整体失败。</summary>
    public static AttachmentBindResult Fail(
        IReadOnlyDictionary<string, AttachmentBindErrorCode> errors) =>
        new(false, [], errors);

    /// <summary>首个失败附件的稳定错误码；全部成功时为 <see cref="AttachmentBindErrorCode.None"/>。</summary>
    public AttachmentBindErrorCode PrimaryErrorCode =>
        Errors.Count == 0
            ? AttachmentBindErrorCode.None
            : Errors.Values.First();
}
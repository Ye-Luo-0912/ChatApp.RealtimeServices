namespace ChatApp.Realtime.Abstractions.Messaging;

public sealed class MessageReactionOptions
{
    public const string SectionName = "MessageReaction";

    /// <summary>单条消息允许的不同 emoji 上限。</summary>
    public int MaxDistinctEmojisPerMessage { get; init; } = 20;

    /// <summary>同一用户对单条消息的反应上限。</summary>
    public int MaxReactionsPerUserPerMessage { get; init; } = 20;

    /// <summary>emoji / short code 最大字符长度。</summary>
    public int MaxEmojiLength { get; init; } = 32;
}

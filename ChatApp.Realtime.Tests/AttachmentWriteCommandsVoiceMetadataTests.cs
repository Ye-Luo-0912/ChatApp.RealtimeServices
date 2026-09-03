using ChatApp.Realtime.Abstractions.Messaging;
using ChatApp.Realtime.Infrastructure.Postgres.Stores;

namespace ChatApp.Realtime.Tests;

/// <summary>
/// VOICE-MSG-2：绑定 SQL 的语音元数据数组构建（发送方快照 → 附件行语音 6 字段）。
/// 覆盖成组有效声明、残缺声明、非语音、越界截断与 id 对齐语义；
/// 落库与回查闭环由 VoiceAttachmentBindTests（Testcontainers）覆盖。
/// </summary>
public sealed class AttachmentWriteCommandsVoiceMetadataTests
{
    [Fact]
    public void Build_NullMetadata_AllSlotsUnset()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1", "a2"],
            attachmentMetadata: null);

        Assert.Equal(["a1", "a2"], arrays.AttachmentIds);
        Assert.All(arrays.IsVoice, v => Assert.Null(v));
        Assert.All(arrays.VoiceCodec, v => Assert.Null(v));
    }

    [Fact]
    public void Build_CompleteVoiceClaim_MarksSlotWithVoiceValues()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                new AttachmentRef
                {
                    AttachmentId = "a1",
                    ContentType = "audio/ogg",
                    IsVoice = true,
                    VoiceCodec = "opus",
                    VoiceContainer = "ogg",
                    VoiceDurationMs = 3_200,
                    VoiceSampleRateHz = 48_000,
                    VoiceChannels = 1
                }
            ]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Equal("opus", arrays.VoiceCodec[0]);
        Assert.Equal("ogg", arrays.VoiceContainer[0]);
        Assert.Equal(3_200L, arrays.VoiceDurationMs[0]);
        Assert.Equal(48_000, arrays.VoiceSampleRateHz[0]);
        Assert.Equal((short)1, arrays.VoiceChannels[0]);
    }

    [Fact]
    public void Build_IncompleteVoiceClaim_Ignored()
    {
        // is_voice=true 但缺 sample_rate：ck_attachments_voice_metadata 禁止，
        // 按无元数据处理（消息必达，绑定不触碰语音列）。
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                new AttachmentRef
                {
                    AttachmentId = "a1",
                    ContentType = "audio/wav",
                    IsVoice = true,
                    VoiceCodec = "pcm",
                    VoiceContainer = "wav",
                    VoiceDurationMs = 3_500,
                    VoiceSampleRateHz = null,
                    VoiceChannels = 1
                }
            ]);

        Assert.Null(arrays.IsVoice[0]);
        Assert.Null(arrays.VoiceCodec[0]);
        Assert.Null(arrays.VoiceContainer[0]);
        Assert.Null(arrays.VoiceDurationMs[0]);
        Assert.Null(arrays.VoiceSampleRateHz[0]);
        Assert.Null(arrays.VoiceChannels[0]);
    }

    [Fact]
    public void Build_NonVoiceReference_Ignored()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                new AttachmentRef { AttachmentId = "a1", ContentType = "image/png" }
            ]);

        Assert.Null(arrays.IsVoice[0]);
    }

    [Fact]
    public void Build_NonPositiveBounds_Ignored()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1", "a2"],
            [
                new AttachmentRef
                {
                    AttachmentId = "a1",
                    ContentType = "audio/ogg",
                    IsVoice = true,
                    VoiceCodec = "opus",
                    VoiceContainer = "ogg",
                    VoiceDurationMs = 0,
                    VoiceSampleRateHz = 48_000,
                    VoiceChannels = 1
                },
                new AttachmentRef
                {
                    AttachmentId = "a2",
                    ContentType = "audio/ogg",
                    IsVoice = true,
                    VoiceCodec = "opus",
                    VoiceContainer = "ogg",
                    VoiceDurationMs = 3_200,
                    VoiceSampleRateHz = -1,
                    VoiceChannels = 1
                }
            ]);

        Assert.Null(arrays.IsVoice[0]);
        Assert.Null(arrays.IsVoice[1]);
    }

    [Fact]
    public void Build_OverlongCodecOrContainer_TruncatedToColumnWidth()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                new AttachmentRef
                {
                    AttachmentId = "a1",
                    ContentType = "audio/ogg",
                    IsVoice = true,
                    VoiceCodec = new string('x', 40),
                    VoiceContainer = new string('y', 40),
                    VoiceDurationMs = 3_200,
                    VoiceSampleRateHz = 48_000,
                    VoiceChannels = 1
                }
            ]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Equal(32, arrays.VoiceCodec[0]!.Length);
        Assert.Equal(32, arrays.VoiceContainer[0]!.Length);
    }

    [Fact]
    public void Build_ReferenceOutsideBindableIds_Ignored()
    {
        // 元数据快照仅对本次可绑定的附件生效；无关 id 不产生任何写入位。
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                new AttachmentRef
                {
                    AttachmentId = "other",
                    ContentType = "audio/ogg",
                    IsVoice = true,
                    VoiceCodec = "opus",
                    VoiceContainer = "ogg",
                    VoiceDurationMs = 3_200,
                    VoiceSampleRateHz = 48_000,
                    VoiceChannels = 1
                }
            ]);

        Assert.Null(arrays.IsVoice[0]);
    }
}

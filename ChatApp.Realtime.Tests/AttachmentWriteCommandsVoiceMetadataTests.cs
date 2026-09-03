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
        // 无波形声明 → 波形位保持 NULL（可选字段，缺省即无波形）。
        Assert.Null(arrays.VoiceWaveformPeaks[0]);
    }

    [Fact]
    public void Build_CompleteVoiceClaimWithWaveform_CarriesPeaksInSlot()
    {
        // 完整语音声明 + 波形：随语音 6 字段同位写入（绑定 UPDATE 同语句写列）。
        var peaks = new byte[] { 12, 96, 255, 48, 7 };
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
                    VoiceChannels = 1,
                    VoiceWaveformPeaks = peaks
                }
            ]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Equal(peaks, arrays.VoiceWaveformPeaks[0]);
    }

    [Fact]
    public void Build_EmptyWaveform_TreatedAsNoWaveform()
    {
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                VoiceWithWaveform("a1", [])
            ]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Null(arrays.VoiceWaveformPeaks[0]);
    }

    [Fact]
    public void Build_OverlongWaveform_DroppedButVoiceFieldsKept()
    {
        // 越界波形（> 64 KiB）按无波形处理；语音 6 字段仍有效写入。
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [
                VoiceWithWaveform("a1", new byte[AttachmentWriteCommands.MaxWaveformPeaksBytes + 1])
            ]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Equal("opus", arrays.VoiceCodec[0]);
        Assert.Null(arrays.VoiceWaveformPeaks[0]);
    }

    [Fact]
    public void Build_BoundLengthWaveform_Kept()
    {
        var peaks = new byte[AttachmentWriteCommands.MaxWaveformPeaksBytes];
        var arrays = AttachmentWriteCommands.BuildVoiceMetadataArrays(
            ["a1"],
            [VoiceWithWaveform("a1", peaks)]);

        Assert.True(arrays.IsVoice[0]);
        Assert.Equal(peaks, arrays.VoiceWaveformPeaks[0]);
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
        // 残缺语音声明：波形随声明整体忽略（"残缺即整体忽略"）。
        Assert.Null(arrays.VoiceWaveformPeaks[0]);
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

    [Fact]
    public void BindSqlCommandText_WritesAndReturnsWaveformColumn()
    {
        // 纯 SQL 层：绑定语句必须写 voice_waveform_peaks（SET + unnest bytea[] + RETURNING），
        // 保证绑定写波形 → 回查带出闭环在语句形状上成立。
        var sql = AttachmentWriteCommands.BuildBindSqlCommandText("realtime.\"attachments\"");

        Assert.Contains(
            "voice_waveform_peaks = COALESCE(m.voice_waveform_peaks, a.voice_waveform_peaks)",
            sql);
        Assert.Contains("@m_voice_waveform_peaks::bytea[]", sql);
        Assert.Contains("voice_waveform_peaks)", sql);
        Assert.Contains("a.voice_waveform_peaks;", sql);
    }

    private static AttachmentRef VoiceWithWaveform(string attachmentId, byte[] peaks) => new()
    {
        AttachmentId = attachmentId,
        ContentType = "audio/ogg",
        IsVoice = true,
        VoiceCodec = "opus",
        VoiceContainer = "ogg",
        VoiceDurationMs = 3_200,
        VoiceSampleRateHz = 48_000,
        VoiceChannels = 1,
        VoiceWaveformPeaks = peaks
    };
}

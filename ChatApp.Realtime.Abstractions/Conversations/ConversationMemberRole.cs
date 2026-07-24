namespace ChatApp.Realtime.Abstractions.Conversations;

/// <summary>
/// 群成员角色（smallint）。单聊成员行默认 <see cref="Member"/>，无权限语义。
/// </summary>
public enum ConversationMemberRole : byte
{
    Owner = 1,
    Admin = 2,
    Member = 3
}

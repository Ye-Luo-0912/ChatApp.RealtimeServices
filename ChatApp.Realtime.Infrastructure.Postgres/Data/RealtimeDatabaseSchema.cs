namespace ChatApp.Realtime.Infrastructure.Postgres.Data;

public sealed class RealtimeDatabaseSchema
{
    public RealtimeDatabaseSchema(string schema)
    {
        Schema = string.IsNullOrWhiteSpace(schema) ? "realtime" : schema.Trim();
    }

    public string Schema { get; }

    public string QuotedSchema => QuoteIdentifier(Schema);

    public string MessagesTableSql => $"{QuotedSchema}.\"messages\"";

    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new InvalidOperationException("数据库架构名不能为空。");
        }

        return identifier.Any(ch => !char.IsAsciiLetterOrDigit(ch) && ch != '_') ? throw new InvalidOperationException("数据库架构名只能包含英文字母、数字和下划线。") : $"\"{identifier}\"";
    }
}

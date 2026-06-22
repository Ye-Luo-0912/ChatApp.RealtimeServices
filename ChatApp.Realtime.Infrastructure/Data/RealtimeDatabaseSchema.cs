namespace ChatApp.Realtime.Infrastructure.Data;

public sealed class RealtimeDatabaseSchema
{
    public RealtimeDatabaseSchema(string schema)
    {
        Schema = string.IsNullOrWhiteSpace(schema) ? "realtime" : schema.Trim();
    }

    public string Schema { get; }

    public string QuotedSchema => QuoteIdentifier(Schema);

    /// <summary>
    /// 获取表示消息表的SQL全称，包括架构名和表名。此属性用于在数据库操作中引用特定的消息表。
    /// </summary>
    /// <remarks>
    /// 该属性返回一个字符串，格式为"[架构名称].messages"，其中"messages"是表名，而架构名称由当前实例的<see cref="QuotedSchema"/>属性提供。
    /// 此属性主要用于构建SQL语句时指定正确的表位置，确保在执行数据库查询或命令时能够准确地定位到目标表。
    /// </remarks>
    public string MessagesTableSql => $"{QuotedSchema}.\"messages\"";

    /// <summary>
    /// 对数据库标识符进行引号处理。
    /// </summary>
    /// <param name="identifier">需要处理的标识符，如表名或列名。</param>
    /// <returns>用双引号包裹后的标识符字符串。如果输入为空或包含非法字符，则抛出异常。</returns>
    /// <exception cref="InvalidOperationException">当标识符为空或者包含非字母数字及下划线字符时抛出。</exception>
    public static string QuoteIdentifier(string identifier)
    {
        if (string.IsNullOrWhiteSpace(identifier))
        {
            throw new InvalidOperationException("数据库架构名不能为空。");
        }

        return identifier.Any(ch => !char.IsAsciiLetterOrDigit(ch) && ch != '_') ? throw new InvalidOperationException("数据库架构名只能包含英文字母、数字和下划线。") : $"\"{identifier}\"";
    }
}

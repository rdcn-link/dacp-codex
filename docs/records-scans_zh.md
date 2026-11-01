# 记录查询：过滤、投影和限制

客户端可为 Item 流附加 records 扫描。服务器按 **过滤 → 投影 → 限制** 执行，只传输匹配记录及请求列。逻辑元数据和 `count(path)` 仍描述完整 Item，不随扫描变化。bytes 视图拒绝扫描设置。

## Python

```python
from dacp import Source, col

with Source.open("dacp://localhost:8815/data1") as ds:
    with ds.open(
        "/reports/orders",
        predicate=(col("id") > 1) & col("amount").is_not_null(),
        columns=["amount"],
        limit=100,
    ) as stream:
        records = stream.as_records()
        try:
            for batch in records:
                print(batch.to_pydict())
        finally:
            records.close()
```

`&`、`|` 组合比较时加括号，用 `~` 否定。Python `and`、`or`、`not` 不能构建表达式。成员和空值检查使用 `col("id").isin([1, 2])`、`.is_null()`、`.is_not_null()`。

## Scala

```scala
import dacp.client.Source
import dacp.client.Expressions.col
import scala.util.Using

Using.resource(Source.open("dacp://localhost:8815/data1")) { ds =>
  Using.resource(ds.open(
    "/reports/orders",
    predicate = Some((col("id") > 1) && col("amount").isNotNull),
    columns = Some(Seq("amount")),
    limit = Some(100L)
  )) { item =>
    Using.resource(item.asRecords()) { records =>
      while (records.next()) println(records.root.contentToTSVString())
    }
  }
}
```

Scala 等于/不等于为 `===`、`=!=`，布尔组合为 `&&`、`||`、一元 `!`。成员检查用 `.isin(Seq(1, 2))`，空值用 `.isNull`、`.isNotNull`。两个 SDK 使用相同 protobuf 表达式树，不接受原始 SQL。

## dacp-shell 中的 SQL

配置 `DACP_CA`、`DACP_CERT`、`DACP_KEY` 后连接运行中的服务器：

```bash
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use data1
```

在 `dacp-shell>` 输入完整单行查询：

```sql
SELECT amount FROM "/reports/orders" WHERE id > 1 LIMIT 10
SELECT amount FROM 2 WHERE id > 1 LIMIT 10
SELECT id, amount FROM "/reports/orders" WHERE NOT (id IN (3, 4)) AND amount IS NOT NULL
SELECT * FROM "/events" WHERE name = 'beta'
```

SQLGlot 在本地把语句解析为类型化扫描表达式，服务器不接收 SQL 文本。源只能是当前 Dataset 中一个双引号包围的公开绝对 Item 路径，或最近一次 `ls` 结果中的无引号正整数编号，例如 `FROM 2`。`ls PATH` 替换编号列表，编号在客户端解析为路径，不作为源标识传给服务器。零和越界编号在打开流前失败。字符串用单引号，双单引号转义，例如 `'O''Brien'`；双引号也可包围包含空格或 SQL 关键字的列名。列名精确区分大小写，SQL 关键字不区分。末尾分号可省略。

支持：

- `SELECT *` 或不重复列名列表。
- 可选 `WHERE`：`=`、`<>`/`!=`、`<`、`<=`、`>`、`>=`、括号、`AND`、`OR`、`NOT`、`IN`/`NOT IN`、`IS NULL`/`IS NOT NULL`。
- 字符串、布尔和带符号数值字面量。按公开 records schema 确定数字类型：浮点列接收浮点字面量，decimal 保留精确文本，不进行字符串转数字。
- 类型化日期时间，如 `DATE '2026-01-02'`、`TIMESTAMP '2026-01-02 12:34:56'`、`TIMESTAMP WITH TIME ZONE '2026-01-02 12:34:56+02:00'`，也支持对应的纯字面量 `CAST`。
- 可选非负整数 `LIMIT`，包括零。

只有 records 支持 SQL。小数字面量需要元数据提供 records schema；原生结构插件应把 records 作为首个视图。没有小数字面量时，可在元数据无 schema 的情况下构建查询，服务器随后根据流 schema 校验。

结果使用 Shell 带边框表格，空结果保留选中列名。`--rows` 是显示上限：指定 SQL LIMIT 时发送两者最小值，否则发送 `--rows`。更大预览可在启动 Shell 时增加 `--rows`。`help select` 提供说明。

当前拒绝关联、别名、限定列引用、CTE、子查询、多语句、排序、分组、聚合、计算列、任意函数和 offset，不静默忽略不支持的子句。路径不存在或无效、未知列、字面量类型不合法或无 records 能力时报告错误，Shell 保持连接。

## 语义与扩展实现

通用表达式、源端下推和插件契约见 [XStream 记录扫描](https://github.com/rdcn-link/xstream-codex/blob/main/docs/records-scans_zh.md)。DACP 负责请求解码、授权和传输，数据层执行扫描。

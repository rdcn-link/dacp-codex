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
dacp-shell dacp://localhost:8815/data1
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

## 表达式语义

- 引用精确匹配公开 Arrow 顶层字段，区分大小写。不支持嵌套遍历、列间比较、算术、排序、聚合和 offset。
- 比较为 `eq`、`ne`、`lt`、`le`、`gt`、`ge`，以及 `in`、`is_null`、`is_not_null`；布尔表达式为 `and`、`or`、`not`。
- 空记录参与比较得到 unknown。布尔逻辑为 true/false/unknown，只有 true 匹配。例如 `NOT (temperature > 20)` 同样排除空温度，需要显式空值检查才能保留。
- 不允许 null 字面量或 `in` 中的 null。空 `in` 恒为 false，包括空记录。
- 字面量携带明确类型和规范文本，保留 decimal/整数精度。整数必须处于列范围；decimal 必须能按声明精度和 scale 表示；浮点列接受整数和有限浮点数。不进行字符串转数字。
- 字符串按 Unicode 标量顺序区分大小写比较，不使用区域排序或正规化。布尔 false 小于 true。NaN 与普通字面量不相等且不满足有序比较；正负零相等。拒绝 NaN/无穷字面量。
- 日期使用日历日期。带时区时间戳按 UTC 瞬间比较，不带时区按本地日期时间比较，不能混用。Python 使用 `date`、`datetime`、`Decimal`；Scala 使用 `LocalDate`、`LocalDateTime`、`Instant`/`OffsetDateTime`、`BigDecimal`。
- binary、list、map、struct 等不支持类型可投影和检查空值，不能与普通字面量比较。目前拒绝字典编码的扫描流。

未知字段、重复/空投影、无效字面量、负限制和畸形表达式明确失败。读流时在输出批次前校验，schema 推断可能先读样本。谓词最多 32 层、1,024 个节点，每个 `in` 最多 10,000 个成员。

投影保留请求列顺序，省略则返回所有列。谓词可以引用未输出的列。限制为非负有符号 64 位整数，跨批次全局生效。零只返回投影后的 schema。到达限制后停止上游并释放资源，读取器可能已缓冲一个批次、JDBC fetch 窗口或 Parquet row group。保留源顺序，但独立读取之间不保证稳定顺序。

## 执行和源端下推

| 数据源 | 当前行为 |
|---|---|
| JDBC SQL | 整数、decimal、布尔和空值谓词使用预编译参数；投影包含输出及过滤列，只有完整谓词已处理时设置 JDBC 最大行数。 |
| DACP Parquet | Avro/Parquet 列投影；支持的有符号整数谓词传给读取器，用于记录及可用 row-group 剪枝。 |
| CSV、NDJSON、Excel | 服务器有界批次过滤、投影，到达限制后提前终止。 |
| 外部提供者或原生结构子对象 | 公共流式回退，或语义等价的插件扫描实现。 |

下推只是优化。SQL/Parquet 下推后公共求值器仍检查原谓词，不支持的部分留给公共求值器。合取可部分下推；析取必须保持所有匹配行的安全超集。限制绝不能放在剩余过滤之前。SQL 中受排序规则影响的字符串比较和浮点比较交给公共求值器。

授权仍针对公开 Item 路径。扫描只包含公开字段和字面量，不包含物理路径、SQL 标识或可执行查询。插件日志应把谓词视为潜在敏感数据。

## 插件实现契约

独立 `dacp-plugin-api` 提供 `Literal`、`Predicate`、`Scan`、`BoundScan`、`ScanStreams`。

- `DatasetAccess.scan(path, request, allocator, listener, ready)` 默认用 `ScanStreams` 变换 `stream(path, "records", ...)`。
- `FormatHandler.scan(source, options, request, allocator, batch, sample, listener, demand)` 默认包装 records 读取器，适用于 directory、mixed 和暂存归档条目。
- 结构化子对象使用 `DatasetAccess.scan`，科学格式插件可查询原生 records，无需暴露物理文件或展开数组。
- 优化后端时先用 `request.bind(schema)` 对完整公开 schema 校验完整请求，再翻译；过滤完成前保留谓词列。需要时使用正确引用的标识和绑定值。
- 覆盖实现负责完整语义。部分下推时通过 `ScanStreams.stream(request, allocator, listener) { filtered => ... }` 传递源批次，用源 root 启动 `filtered` 并通过它输出，使其负责剩余过滤、投影和全局限制。保留原谓词做最终检查。
- 保持同步异常传播、背压/取消及 `finally` 清理。到达限制后在当前批次结束时展开并关闭读取器，不吞掉 `start`、`putNext` 或 demand 的异常。

传输、语义、下推和资源测试见 [ScanSpec](../dacp-server/src/test/scala/dacp/ScanSpec.scala)、[SqlSpec](../dacp-server/src/test/scala/dacp/SqlSpec.scala)。

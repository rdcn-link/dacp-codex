# 在一个 DACP 节点上提供文件与 SQL 表

[English](README.md)

从项目根目录运行，需要 JDK 17、sbt、Python 和 OpenSSL。读取 Dataset 无需 Docker。

```bash
python3 -m venv .venv
.venv/bin/pip install -e ./dacp-client
bash scripts/certs.sh
cd dacp-server
sbt 'run ../examples/sql/server.yaml'
```

在另一个终端中，从项目根目录运行：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python examples/sql/client.py
```

这会启动一个节点，提供逻辑 Dataset `dacp://localhost:8815/files` 和 `dacp://localhost:8815/sales`。H2 是嵌入式 Java 数据库，存储于 `examples/sql/sales`。其私有 JDBC 数据源 `warehouse` 使用 `init.sql` 初始化演示表。客户端访问别名 `/reports/orders` 和 `/clients`；物理表名和文件路径保留在服务器端。

```python
from dacp import Source

with Source.open("dacp://localhost:8815/sales") as ds:
    for item in ds.list("/"):
        print(item.path, item.kind)
    print(ds.list("/reports"))
    for batch in ds.open("/reports/orders").as_records():
        print(batch.to_pydict())
```

`list()` 递归返回所有 Item；`list(path)` 返回文件夹的直接子项。所有路径以 `/` 开头。文件和 SQL 表都成为 `data` Item，父级分组为 `folder` Item。可以用 `kind: folder` 声明空文件夹。`sales` Dataset 使用 `provider: sqldb` 和 `source.connection: warehouse`，其 `tables` 映射只需填写 `table`。`files` Dataset 使用 `mixed`。一个 mixed Dataset 可以组合文件与 SQL 映射。

## PostgreSQL

已捆绑 PostgreSQL 和 H2 JDBC 驱动。替换 `sources` 下的私有 `warehouse` 数据源，并更新其显式公开映射：

```yaml
datasets:
  sales:
    provider: sqldb
    source:
      connection: warehouse
    tables:
      /reports/orders: {table: orders}
      /clients: {table: customers}
sources:
  warehouse:
    provider: jdbc
    settings:
      url: jdbc:postgresql://db-host:5432/sales?sslmode=verify-full
      user_env: DACP_DB_USER
      password_env: DACP_DB_PASSWORD
      schema: public
      tables: [orders, customers]
      fetch_size: 256
      query_timeout_seconds: 60
```

启动节点前设置上述环境变量，并为 PostgreSQL 驱动配置可信的服务器 CA，以使用 `verify-full`。数据库账号需要对公开表具有 SELECT 权限，并对 schema 具有 USAGE 权限。凭据保留在服务器上，不会发送给 DACP 客户端。也支持直接填写 `user` / `password`；环境变量字段优先，引用的变量缺失时配置加载失败。每个私有 SQL 数据源描述一个数据库 schema。如需组合多个数据源，可以使用 `provider: mixed` 并配置 Item 级 `sql` 绑定。

使用 `provider: directory` 和 `source.root` 自动发现文件，或显式配置 `sqldb` / `mixed` 的公开 Item 映射及私有 `sources`。未映射的私有数据源和表不会公开。文件映射使用 `{file: /private/path.csv}`；SQL 映射使用 `{sql: warehouse, table: private_table}`。客户端可见的名称和路径是独立选择的别名。每个私有 SQL 数据源支持：

| 字段 | 含义 |
|---|---|
| `url` | JDBC 连接 URL |
| `user`, `password` | 可选的服务器端凭据 |
| `user_env`, `password_env` | 存储凭据的环境变量名称 |
| `schema` | 精确的数据库 schema；默认使用驱动当前 schema |
| `tables` | 可选的精确表名白名单，区分大小写；省略则允许映射 schema 中任意表，`[]` 不允许任何表；所有表仍需显式公开别名 |
| `fetch_size` | JDBC 获取窗口，默认为 `batch_size` |
| `query_timeout_seconds` | 语句超时，默认为 60 秒，必须为正数 |

URI 前缀授权作用于公开逻辑地址。Dataset 前缀授予列表和读取权限；data Item 前缀允许直接读取，但不授予 Dataset 列表权限。SQL 映射具有 `kind: data`、`format: sql`、`views: [records, bytes]` 及 Arrow schema 元数据。客户端不能提交任意 SQL、过滤条件、连接、写入或物理表名。服务器端表映射必须匹配 JDBC 元数据，并作为标识符加引号。数据源绑定不会出现在列表或 ticket 中，数据服务错误也不泄露底层路径或标识符。列名和数据值仍属于返回内容。

SQL 数据项在正常列举和打开时，`record_count` 保持未知。显式调用 `ds.count("/reports/orders")` 会执行 `COUNT(*)`，返回精确计数，空表返回零。由于 JDBC 没有可移植的内容修改时间或序列化字节大小，`last_modified_time_ms` 和 `total_size_bytes` 保持缺失。计数可能扫描大表，并遵守 `query_timeout_seconds`；元数据和后续读取不共享同一快照。所有文件夹属性均保持未知，包括空文件夹，且不会访问子 Item。

## 类型与流语义

支持的 SQL 类型映射到 Arrow 如下：

| SQL | Arrow |
|---|---|
| 整数类型 | 有符号 int64 |
| REAL / FLOAT / DOUBLE | float64 |
| BOOLEAN / BIT | bool |
| 字符类型 | utf8 |
| 二进制类型 | binary |
| DECIMAL / NUMERIC，声明精度为 1–38，小数位数为 0–精度 | decimal128 |
| DATE | date32 |
| TIME | time64，微秒 |
| TIMESTAMP | timestamp，微秒，无时区 |
| TIMESTAMP WITH TIME ZONE | timestamp，微秒，UTC |

保留空值和 JDBC 列的可空性，时间精度低于微秒的部分会截断。目前明确拒绝数组、LOB、任意厂商特有类型、无约束 decimal 和带时区 time，不会静默转换类型。其他 JDBC 引擎需要先在 `build.sbt` 中添加驱动，并验证 schema 元数据、类型映射和游标行为。

`as_records()` 直接流式返回表记录。表没有原始文件表示，因此 `as_bytes()` 在标准单二进制列 Flight 视图中流式返回 **Arrow IPC 序列化**字节。可以使用 `pyarrow.ipc.open_stream(ds.open("/reports/orders").as_bytes())` 消费；它不是 CSV、SQL 文本或数据库备份。`format_convert@1.0` 仍只接受 CSV/NDJSON；SQL 表转换不在此次适配器变更范围内。

每个请求独占 JDBC 连接、语句和结果集。读取请求使用只读事务和只向前游标。PostgreSQL 使用 autocommit=false 和正数 fetch size，避免缓冲整个结果（见[驱动文档](https://jdbc.postgresql.org/documentation/query/)）。Arrow 分配受批次大小约束，JDBC 缓冲行为取决于驱动，单个大值仍会占用内存。SQL 结果没有隐含行顺序。列表/FlightInfo 与 DoGet 使用不同连接，因此元数据和数据不共享同一快照。重新打开的流从头读取，不支持重试、检查点或去重。在获取数据或批次之间检查取消；正在执行的数据库操作可能持续到驱动或查询超时。

## 验证

```bash
cd dacp-server
sbt test
# 从项目根目录运行：真实 mTLS Python/Flight/H2 与文件读取验证
bash scripts/test-sql.sh
```

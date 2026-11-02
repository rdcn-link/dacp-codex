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

## 配置数据库后端

PostgreSQL 连接配置、JDBC 参数、SQL 到 Arrow 的类型映射、游标与流式读取语义，见 [XStream JDBC 数据集指南](https://github.com/rdcn-link/xstream-codex/blob/main/docs/jdbc-datasets_zh.md)。将其中的数据目录配置与 DACP 的监听、TLS 和凭据配置合并即可发布。数据库凭据保留在节点上；远程调用权限见 [DACP 认证与访问控制](../../docs/security_zh.md)。

## 验证

```bash
cd dacp-server
sbt test
# 从项目根目录运行：真实 mTLS Python/Flight/H2 与文件读取验证
bash scripts/test-sql.sh
```

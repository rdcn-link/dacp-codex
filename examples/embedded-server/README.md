# 嵌入式 DACP Server 示例

[English](README_en.md) · [嵌入接口](../../docs/embedded-server.md) · [目录契约](../../docs/dataset-catalogs.md)

示例应用在自己的 JVM 中启动 DACP Server，直接注入 `DatasetCatalog`，无需 ServiceLoader 注册。服务器启动后，应用将 `reports` 加入并发注册表，公开 `/orders` CSV Item。运行期间业务程序可以继续添加或删除注册记录。

从项目根目录执行，8815 端口须空闲：

```bash
bash scripts/certs.sh
(cd examples/embedded-server && sbt 'run ../certs ../../examples/data/nested/orders.csv')
```

另开终端，从项目根目录连接：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use reports
```

在 shell 中使用 `ls /`、`cat /orders`。前台示例用 Ctrl+C 停止。示例应用自行安装关闭钩子，嵌入 API 不自动安装。

需要 JDK 17、sbt 1.10.x 和已安装的 Python SDK/shell；本示例只读取 CSV，不需要 Docker。项目依赖 `../../dacp-server` 源码工程，JVM 的 Arrow 参数见 [build.sbt](build.sbt)。完整源码见 [EmbeddedServerExample.scala](src/main/scala/example/EmbeddedServerExample.scala)。

目录返回的每个 `PublishedDataset` 由请求拥有，服务器在访问完成后关闭它。目录刷新不主动关闭已有访问句柄，但不保证底层数据不发生变化。若宿主程序使用共享数据库连接池，应保留其所有权，在服务器停机后自行关闭。数据库目录的独立插件示例见 [JdbcCatalogProvider.scala](../plugins/src/main/scala/example/JdbcCatalogProvider.scala)。

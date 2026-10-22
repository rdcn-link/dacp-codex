# 动态 Dataset 目录

[English](dataset-catalogs_en.md) · [插件体系](plugins.md) · [嵌入式服务器](embedded-server.md)

`DatasetCatalog` 决定服务器当前发布哪些 Dataset；`DatasetProvider` 决定一个 Dataset 如何访问内容。数据集名称来自数据库、业务服务或用户程序时，使用目录插件；单个 Dataset 内的动态 Item 列表仍由 Dataset provider 实现。

## 命令行部署

默认的 `static` 目录从配置中的 `datasets` 构建数据集。动态目录使用以下配置，不能同时声明非空的 `datasets`：

```yaml
plugin_directory: ./.dacp-plugins
dataset_catalog:
  provider: example-jdbc-catalog
  source:
    jdbc_url: jdbc:postgresql://localhost:5432/metadata
    user_env: DACP_CATALOG_USER
    password_env: DACP_CATALOG_PASSWORD
```

以上是独立示例插件 `example.JdbcCatalogProvider` 的参数，完整配置还需要监听地址和 TLS 等常规节点设置。`source` 由插件解释和校验；核心不包含 JDBC 专用的目录分支。示例源码位于 [JdbcCatalogProvider.scala](../examples/plugins/src/main/scala/example/JdbcCatalogProvider.scala)。构建和安装方式见[插件示例](../examples/plugins/README.md)。将 JDBC 驱动及其依赖随插件安装到 `plugin_directory`，例如 PostgreSQL JDBC 驱动；目录表连接与普通 SQL 数据访问配置相互独立。

示例查询以下登记表；表的创建和数据维护由业务系统负责，插件不会自动建表：

```sql
CREATE TABLE dacp_datasets (
  name VARCHAR(255) PRIMARY KEY
);
CREATE TABLE dacp_items (
  dataset_name VARCHAR(255) NOT NULL,
  item_path VARCHAR(1024) NOT NULL,
  file_path VARCHAR(4096) NOT NULL
);
INSERT INTO dacp_datasets VALUES ('reports');
INSERT INTO dacp_items VALUES ('reports', '/orders', '/srv/data/orders.csv');
```

客户端访问 `dacp://host:8815/reports`，其中 `/orders` 是公开 Item，`file_path` 是节点本地私有路径。示例每次列举都查询 `dacp_datasets`；每次打开都查询相应的 `dacp_items` 并创建独立的数据访问快照。增删目录记录不需要重启。JDBC 查询采用参数绑定和 10 秒查询超时，查询完成后关闭连接；后端异常作为请求错误返回，不伪装成空目录。需要缓存、一致性事务或连接池的插件自行实现这些策略。

## 插件契约与注册

公开类型位于 `dacp-plugin-api`：

```scala
trait DatasetCatalog extends AutoCloseable {
  def list(): Seq[DatasetInfo]
  def open(name: String): PublishedDataset
  // 另提供带 check: () => Unit 的重载，以便执行取消检查。
}

trait DatasetCatalogProvider {
  def id: String
  def create(settings: Map[String, Object], context: CatalogContext): DatasetCatalog
}
```

`DatasetInfo` 只包含公开名称 `name`。`CatalogContext.createDataset(definition)` 复用已安装的 Dataset provider；传入 `FileDatasets.mixed` 可复用文件与 SQL 映射；原生后端也可以直接实现 `PublishedDataset`。不要将科学数组、图像或分组隐式转换成记录。

实现类提供公开无参构造函数，并在 JAR 中加入：

```text
META-INF/services/dacp.DatasetCatalogProvider
```

文件内容为完整类名，例如：

```text
example.JdbcCatalogProvider
```

启动时根据配置的 `provider` 匹配插件 `id`，调用 `create(source, context)` 一次。缺失或重复的 provider、无效配置使启动失败；不需要修改核心注册表或重新编译服务器。

## 生命周期、授权与发现

- 目录须支持并发请求。`list` 返回当前快照；名称须符合节点命名规则且不得重复。目录查询失败必须抛出异常。
- `open` 找不到名称时抛出 `FileNotFoundException`。返回值是调用方拥有的 `PublishedDataset` 租约；服务器关闭请求访问句柄后关闭该租约。不要把共享的可关闭对象直接交给多个请求。
- 已取得的租约须保持有效，即使目录记录之后被删除或刷新。文件或数据库本身消失仍可能导致读取失败；这不是内容快照或持久结果保证。
- 覆盖带 `check` 参数的重载，在查询循环和耗时步骤中检查取消；数据读取仍遵守现有背压和取消契约。
- 服务器关闭目录实例。目录仅释放自己拥有的资源；用户注入的共享连接池由用户管理。
- 目录信息无需额外的数据或 Union 执行授权，仍使用 Flight mTLS。已发布的数据集默认允许读取，仍验证命名空间和资源路径。

目录发现使用标准 Flight `ListFlights`，默认使用空 criteria；dacp-server 还接受 `local` criteria，供仅连接节点的 Shell 使用。每条 `FlightInfo.app_metadata` 为 `DataSet` protobuf，包含公开名称、URI 和包含用户自定义 JSON 对象字符串的 `description`，不包含 Item 内容。公开描述、Shell 和 Union Web 按节点查看目录见[数据集目录](dataset-directory.md)。

在项目根目录使用 Python 的标准 Flight 接口列举数据集：

```python
from pathlib import Path
import pyarrow.flight as flight

with flight.FlightClient(
    "grpc+tls://localhost:8815",
    tls_root_certs=Path("examples/certs/ca.crt").read_bytes(),
    cert_chain=Path("examples/certs/client.crt").read_bytes(),
    private_key=Path("examples/certs/client.key").read_bytes(),
) as client:
    for info in client.list_flights():
        print(info.descriptor.path[1].decode("utf-8"))
```

## 契约测试

运行 `(cd dacp-server && sbt test)`。`DatasetCatalogSpec` 使用 ServiceLoader 加载独立 JDBC 示例，通过真实 Flight 验证增删目录、数据读取、公开访问、后端故障和租约保留；同时验证无效名称、重复名称、取消、初始化失败及关闭次数。插件作者应覆盖同类场景，并验证自己的缓存、数据库事务与连接池资源管理。

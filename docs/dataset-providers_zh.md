# Dataset 提供者

Dataset 是逻辑命名空间。提供者决定如何构建它；客户端始终使用公开路径以及 `list`、`open`、`count`。内置后端实现位于 [DatasetPublication.scala](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-files/src/main/scala/dacp/DatasetPublication.scala)。

## 内置提供者

| 提供者 | 必需 source | 映射键与设置 |
|---|---|---|
| `directory` | `root`：私有文件系统目录 | `reader_overrides`：按逻辑路径指定可选读取设置 |
| `sqldb` | `connection`：私有 `sources` 中的 JDBC 名称 | `tables`：以 `{table: PRIVATE_TABLE}` 映射公开别名 |
| `mixed` | 不需要 source 设置 | `items`：显式 `{file: ...}`、`{sql: ..., table: ...}` 或 `{kind: folder}` |
| `mounted` | 不需要 source 设置 | `mounts`：为每个公开前缀指定嵌套 `dataset` 和可选 `root` |

```yaml
sources:
  warehouse:
    provider: jdbc
    settings:
      url: jdbc:postgresql://localhost/warehouse
      user_env: WAREHOUSE_USER
      password_env: WAREHOUSE_PASSWORD
datasets:
  documents:
    provider: directory
    source:
      root: /home/test/dataset1
    reader_overrides:
      /events.json: {format: ndjson}
  sales:
    provider: sqldb
    source:
      connection: warehouse
    tables:
      /customers: {table: PRIVATE_CUSTOMERS}
  research:
    provider: mixed
    items:
      /measurements: {file: /private/results.csv}
      /customers: {sql: warehouse, table: PRIVATE_CUSTOMERS}
  workspace:
    provider: mounted
    mounts:
      /documents:
        dataset:
          provider: directory
          source: {root: /srv/documents}
        root: /
```

目录根必须在启动时存在。相对路径基于服务器工作目录解析。公开名称相对于目录根；忽略所有符号链接，包括指向根目录内部的链接。不支持的文件系统条目和不能构成合法逻辑路径的名称会跳过。空目录显示为 folder。容器提供子对象和原文件字节，行为与 mixed Dataset 相同。`reader_overrides` 只接受 `format`、`schema` 和处理器定义的 `options`，不能增加数据源或别名。

每个 directory 请求重新发现相关路径。`list()` 递归遍历根目录，`list(path)` 读取普通目录的直接子项。普通文件夹元数据不遍历后代。容器目录枚举有格式对应的扫描成本，不需要文件监控器或跨请求索引。

`count_file_records_on_startup: true` 为 mixed 文件映射和启动时发现的 directory records 文件记录计数；不包括 SQL 表、容器和归档条目。后续新增文件只发现不计数；大小或修改时间变化后，已保存计数失效。显式 `count` 不依赖保存计数。启动计数需要主动开启，可能昂贵。

SQLDB 使用一个连接，但仍要求公开别名，不自动发布私有表名。每个 Dataset 必须明确声明唯一 `provider`。mixed 组合文件/JDBC 源；mounted 通过公共 Dataset 契约组合任意提供者，保持子对象能力和结构。挂载前缀必须非根且不能重叠，子根默认为 `/`；嵌套构建深度最多 32 层。

Directory 和 SQLDB 拒绝 `items`，应使用各自映射键。

## 扩展构建逻辑

实现 `xstream.data.DatasetProvider`，使用唯一的小写 `id` 和公开无参构造器。在 JAR 中注册：

```text
META-INF/services/xstream.data.DatasetProvider
```

文件每行填写一个提供者完整类名。将 JAR 和私有依赖放入 `plugin_directory` 并重启。针对 `dacp-plugin-api` 编译，依赖范围为 `Provided`，不依赖服务器实现。重复或未知 ID 导致配置加载失败。插件自行解释私有 `source` 和映射设置，核心不增加硬编码的 Dataset 类型。

提供者级参数放在 `source`，嵌套 YAML 映射和列表保持 Java 集合表示。插件必须校验必需值、类型和未知设置。按逻辑路径配置默认使用 `items`；可覆盖 `DatasetProvider.itemSettingsKey` 指定其他键，名称以字母开头，只含小写字母、数字和下划线，`provider`、`source` 保留。解析器把内容传给 `DatasetDefinition.items`，拒绝其他 Dataset 级键。完整实现、注册和参数配置见 [CSV 提供者示例](developers/README_zh.md#自定义-dataset-provider)。

生命周期如下：

1. `create(definition, context)` 校验私有设置，为服务器配置构建 `PublishedDataset`，可初始化发现或发布元数据。日志和公共错误中不能出现凭据或物理标识；`DatasetDefinition.toString` 隐去设置。
2. `PublishedDataset.open(check)` 创建请求级 `DatasetAccess`，取消请求时通过回调中止发现。
3. `DatasetAccess` 实现递归列表、直接子项列表、元数据、精确计数和流式交付。`view(path, kind)` 返回可选的类型化 `DataView`，通过 `open(selection, StreamContext)` 获得单次消费的 `DataStream`。读写前检查取消，在成功、失败和取消时关闭资源，见[类型化数据流](https://github.com/rdcn-link/xstream-codex/blob/main/docs/data-streams_zh.md)。

用 `Item.views = Some(Seq("records", "bytes"))` 显式声明原生能力，无需依赖文件格式处理器。records 开始前必须有匹配的 Arrow schema。进程内 bytes 输出 `ByteChunk`，网络适配器使用单 binary 列的 Arrow 信封。普通 folder 没有视图，其大小、时间和记录数保持未知。不存在的资源抛 `FileNotFoundException`；格式失败使用固定消息的 `FormatFailure`，不能包含私有信息。核心实施 URI 授权并清理错误，不应抛出包含后端诊断的传输异常。

原生提供者可以直接输出流，MongoDB 不必伪装成文件或 JDBC 连接。目前没有内置 MongoDB 实现。[TestDatasetProvider](../dacp-server/src/test/scala/dacp/TestDatasetProvider.scala) 展示 ServiceLoader 发现及通过相同 Flight 协议输出原生流。

Dataset 提供者、[归档读取器](format-handlers_zh.md#归档读取器)和[格式处理器](format-handlers_zh.md)分别负责构建命名空间、浏览容器和解释内容。mixed/sqldb 复用 `NamespaceAdapter`；directory 按发现的文件系统路径构建请求级命名空间。任务输出是临时 Item 流，不进入 Dataset 命名空间。

`DatasetContext` 提供批次设置、通用 `sources`、当前 `PluginEnvironment` 和 `createDataset`，无需暴露服务器实现。后端插件自行校验源设置。核心关闭请求句柄；`PublishedDataset.close` 释放提供者级资源。贡献者测试可使用 [PluginChecks](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/xstream-data-core/src/main/scala/xstream/data/PluginChecks.scala)，另见[独立示例](../examples/plugins/README_zh.md)。

## 记录扫描

打开 `DataViews.Records` 时传入共享 `Scan`，支持过滤、投影和限制。提供者可以下推，但必须保留必要的剩余过滤。`BatchDatasetAccess.readScan` 提供有界批次默认实现。见[记录扫描契约](records-scans_zh.md#插件实现契约)。

## 动态 Dataset 名称

`DatasetProvider` 处理单个 Dataset。需要从数据库或应用发现服务器级 Dataset 列表时，使用 [DataCatalog](dataset-catalogs_zh.md)。静态 `datasets` 使用内置 static catalog；动态 catalog 自行维护列表，Item 浏览和读取契约相同。

逐 Item 对象和游标句柄使用 `ItemDatasetAccess`、`ItemAccess`，见[访问契约](dataset-access_zh.md)。`PublishedItem` 属于内置文件/JDBC 插件，不是原生后端必需的表示。

# DACP 插件 API

[English](README.md)

为 Dataset 目录、Dataset provider、文件处理器、结构化文件浏览、容器读取器和算子启动执行器提供共享的 Scala/JVM 契约。本模块依赖 Arrow、Scala 和通用 JSON 工具 Jackson，不依赖服务器、SQL 驱动、Docker、Excel 或 HTTP 的具体实现。

在项目根目录运行：

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd examples/plugins && sbt package)
```

贡献插件以 `Provided` 方式依赖 `io.github.rdcn-link:dacp-plugin-api_2.13:0.1.0-SNAPSHOT` 编译，服务器在运行时提供匹配的 API。本 API 尚未发布，契约变化时应同步更新插件和核心。

- `DataCatalog`、`DataCatalogProvider`、`CatalogContext`、`DatasetInfo`：动态发现 Dataset 并取得独立访问租约，见[动态目录](../docs/dataset-catalogs_zh.md)。
- `DatasetProvider`、`DatasetDefinition`、`DatasetContext`、`PublishedDataset`、`DatasetAccess`：构建和访问原生命名空间。
- `ItemDatasetAccess`、`ItemAccess`：按 Item 打开对象或游标，统一关闭内容句柄，见[访问契约](../docs/dataset-access_zh.md)。
- `FormatHandler`、`FileOptions`、`Records`：解释文件并流式传输 Arrow 批次。
- `StructuredFileHandler`：将文件内部的逻辑对象直接暴露为命名空间。
- `OperatorExecutor`、`OperatorProgram`、`OperatorCommand`：验证算子制品并选择容器启动命令，参见[算子打包](../docs/operator-packaging_zh.md)。
- `ContainerReader`：枚举归档条目，并暂存单个文件供已有格式读取器使用。
- `Item`、`LogicalPath`、`PluginSchemas`、`FormatFailure`：公共和内部通用契约。
- `PluginChecks.listing`：供贡献者测试复用的元数据检查，无需读取或统计内容。

内置实现位于 [plugins/](../plugins/README_zh.md)。本模块只保留契约、发现、通用记录/扫描和资源工具，不包含具体数据源或格式解析器。`DatasetContext.createDataset` 可组合已安装的 Provider；`PublishedItem` 和 `SqlConfig` 属于文件/JDBC 插件；核心使用通用 `SourceDefinition(provider, settings)`。

配置、注册、生命周期和测试见[插件指南](../docs/plugins_zh.md)及[独立示例](../examples/plugins/README_zh.md)。请使用与服务器相同的 Scala 版本编译。第三方库应保留在插件中；将所需依赖与插件一起打包，但不要复制服务器已提供的 API。

记录查询契约包括 `Scan`、带类型的 `Literal`/`Predicate` 树、基于 schema 的验证，以及使用有界内存进行过滤、投影和提前终止的 `ScanStreams`。Dataset Provider 的 records 视图接收 `Scan`；可选的 `BatchDatasetAccess` 工具覆盖 `readScan`，文件处理器覆盖 `scan` 来优化数据源访问，同时保持公共语义。参见[记录查询](../docs/records-scans_zh.md)。

算子 `OperatorProgram.configuration` 接收插件自有的 `executor_config`，`inputs`、`outputs` 提供各端口支持的表示集合。插件验证配置，核心不解析具体执行器参数。参见[原生命令](../docs/native-commands_zh.md)。

`OperatorCommand.executableScratch` 可请求 `/operator-work` 上限 512 MiB 的可执行 tmpfs，用于原生制品的私有执行副本；普通 `/tmp` 保持不可执行。资源生命周期由服务器管理。

`PluginEnvironment` 按应用/服务器管理发现、注入实现和生命周期。`ByteSource` 解耦格式读取与本地路径。`ExecutionStore`/`ExecutionStoreProvider` 定义执行元数据持久化。注册和配置见[插件指南](../docs/plugins_zh.md)。

- `MaterializationStoreProvider`、`MaterializationStore`、`MaterializationWriter`、`MaterializedTask`、`MaterializedChunks`：任务输出原子提交、租约、不透明帧块及重放完整性，见[物化契约](../docs/materialization_zh.md)。

## 类型化数据流

`DataView[K,S]` 根据类型化选择条件打开 `DataStream[K]`，通过 `DataConsumer[K]` 同步传递描述信息与借用的数据块。bytes 使用 `ByteChunk`，records 使用 `RecordBatch`；插件无需实现网络监听器。Dataset 提供者通过 records 视图接收 `Scan`，可选的 `BatchDatasetAccess` 工具重写 `readScan`，文件处理器仍重写 `scan` 实现源端优化。

`ScientificViews` 定义保留科学结构的进程内数组、图像和序列契约，尚未内置 NetCDF、FITS、FASTA 解析器及科学数据网络编码。详见[类型化数据流](../docs/data-streams_zh.md)。

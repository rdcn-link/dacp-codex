# DACP 内置插件

[English](README.md)

所有内置扩展实现位于本目录。插件依赖 `dacp-plugin-api`，不依赖 `dacp-server`，通过 JAR 内的 `META-INF/services/` 注册；与外部贡献插件使用相同的发现机制，核心注册表不列举内置类。

| 模块 | 实现与注册 ID | 实现专有依赖 |
|---|---|---|
| `xstream-data-files` | `DatasetProvider`：`directory`、`sqldb`、`mixed`；文件/JDBC 读取与组合命名空间 | H2、PostgreSQL JDBC 驱动 |
| `xstream-data-formats-basic` | `FormatHandler`：`csv`、`ndjson`、`json`、`bin` | Apache Commons CSV |
| `xstream-data-parquet` | `FormatHandler`：`parquet`，含扫描下推 | Parquet、Hadoop |
| `xstream-data-excel` | `FormatHandler`：`excel` | Apache POI |
| `xstream-data-archives` | `ContainerReader`：`zip`、`gzip`、`tar`、`tar-gzip` | Apache Commons Compress |
| `xstream-data-catalog-static` | `DataCatalogProvider`：`static` | 无额外依赖 |
| `xstream-plugin-operator-executors` | `OperatorExecutor`：`container`、`jvm`、`python`、`command` | 无额外依赖 |
| `xstream-data-mounted` | `DatasetProvider`：`mounted`；通用命名空间组合 | 无额外依赖 |
| `dacp-plugin-execution-file` | `ExecutionStoreProvider`、`MaterializationStoreProvider`：`file`；原子执行元数据、带配额/过期/完整性校验的输出物化 | 无额外依赖 |

文件、JDBC 与 mixed 在同一 Dataset 插件中共享别名和目录实现；格式读取器独立打包，Dataset 插件不依赖 POI、Parquet 或归档实现类。JSON 默认提供原始 bytes；NDJSON 提供 records。算子程序与共享运行时仍位于 `operators/`、`examples/operators/`，它们在容器中执行，不是加载进服务器的插件。

## 构建和运行

从仓库根目录运行：

```bash
(cd /path/to/xstream-codex/xstream-data/xstream-data-files && sbt package)
(cd /path/to/xstream-codex/xstream-data/xstream-data-excel && sbt package)
(cd /path/to/xstream-codex/plugins/xstream-plugin-operator-executors && sbt test)
(cd dacp-server && sbt test 'Universal / stage')
```

服务器编译时只依赖公共 API 和协议等核心模块。标准发行包通过运行时依赖携带以上插件及其依赖，`sbt run` 与打包启动均可直接使用内置能力，无需重复复制内置插件到安装目录。集成测试在服务器模块验证真实 Flight、别名、记录/字节流、扫描、资源清理和取消。

第三方插件编译时以 `Provided` 方式依赖 `dacp-plugin-api`，将插件 JAR 和服务器尚未提供的私有依赖直接放入安装目录，重启加载。例如：

```yaml
plugin_directory: ../.dacp-plugins
datasets:
  files:
    provider: directory
    source: {root: /srv/data}
```

`plugins/` 是受版本控制的源码目录；`.dacp-plugins/` 是 Git 忽略的本地安装目录。服务器不会递归扫描源码目录。不要重复安装标准包已附带的相同插件 ID。目录、后端、格式、归档和启动器的服务文件分别为 `xstream.data.DataCatalogProvider`、`xstream.data.DatasetProvider`、`xstream.data.FormatHandler`、`xstream.data.ContainerReader`、`dacp.OperatorExecutor`。

## 架构边界与扩展

`dacp-plugin-api` 提供契约、通用 Arrow 扫描与记录工具、发现和资源工具；`dacp-server` 保留 Flight、授权、请求与任务生命周期。核心 `sources` 只保存 provider/settings；Dataset 插件校验 JDBC 设置并实现连接、查询和类型映射。没有为 MongoDB、S3、NetCDF 或 FITS 提供生产实现。

新增现有扩展点的插件只需注册、安装和配置，无需修改核心或客户端。接口和示例见[插件指南](../docs/plugins_zh.md)、[Dataset/Item 访问](../docs/dataset-access_zh.md)、[独立贡献示例](../examples/plugins/README_zh.md)和[执行器说明](https://github.com/rdcn-link/xstream-codex/blob/main/plugins/xstream-plugin-operator-executors/README_zh.md)。

[输出物化配置与插件契约](../docs/materialization_zh.md)。

# 开发者指南

[English](README_en.md) · [文档目录](../README.md) · [用户指南](../users/README.md)

面向实现算子、Dataset provider、格式处理器、执行器、SDK 或服务端的贡献者。连接节点、浏览数据、配置内置 provider 和运行作业请先阅读用户指南。除非另有说明，命令均从项目根目录执行。

## 架构与扩展点

核心负责契约、发现与生命周期、传输、授权、命名空间校验、取消和资源管理。插件负责领域行为、私有参数和依赖。新增已有扩展点的实现不应修改核心注册表或客户端；内置与外部插件使用相同的注册契约。

| 模块 | 职责 |
|---|---|
| `dacp-plugin-api` | Provider、格式、扫描及执行器公开契约 |
| `dacp-server` | Flight 服务、命名空间、协调与资源生命周期 |
| `dacp-client`、`dacp-client-scala` | 客户端 API 与 shell |
| `dacp-operator-runtime` | JVM 算子运行时及内置实现 |
| `dacp-operator-executors` | 容器启动器与原生命令桥接器 |
| `dacp-operator-repository` | 不可变发布、公开搜索与下载 |
| `proto/dacp.proto` | 共享线协议消息 |

## 编写与发布算子

- [算子编写指南](../operator-authoring.md)：Scala/Python 完整源码、构建及上传。
- [原生命令](../native-commands.md)：stdin/stdout 适配、可执行制品与插件配置。
- [打包与执行器](../operator-packaging.md)：共享依赖、不可变制品、仓库 schema 与注册。
- [流契约](../operator-streams.md)：命名端口、帧、背压、取消和调度边界。
- [DEBUG 源码](../operator-source.md)：Python/Scala 编译启动契约；不支持 Java 原始源码。

算子输出为临时流，不注册结果 Dataset，不提供保存结果模式或输出 TTL。科学数据须保留结构：已实现 bytes 和 records；新增数组或图像表示须先定义共享契约。通用流水线调度与 fan-out 尚未实现。

## 扩展 Dataset 与格式

参见[插件贡献要求](../plugins.md)、[Dataset provider 契约](../dataset-providers.md)、[格式及容器处理器](../format-handlers.md)和[记录查询语义](../records-scans.md)。下面的示例说明 provider 实现、配置责任与服务注册。

### 自定义 Dataset provider

用户定义的 provider 是服务端 Scala/JVM 插件。`provider` 选择插件，`source` 传入该 Dataset 实例的私有参数；同一个 provider 可用不同参数构建多个 Dataset。插件负责校验参数，客户端只看到公开路径和数据。

下面是一个可发布单个 CSV 文件的完整 provider 示例，固定公开路径为 `/records`：

```scala
package example

import dacp._
import java.nio.file.Path

class CsvFileProvider extends DatasetProvider {
  val id = "csv-file"

  def create(d: DatasetDefinition, context: DatasetContext): PublishedDataset = {
    require(d.source.keySet.subsetOf(Set("file")),
      "unknown csv-file parameter")
    require(d.source.contains("file"), "csv-file requires source.file")
    require(d.items.isEmpty && d.bindings.isEmpty,
      "csv-file does not accept item mappings")
    val file = Path.of(d.source("file").toString)
    context.publish(Seq(PublishedItem(
      "/records", file = Some(file),
      options = FileOptions(format = Some("csv"))
    )))
  }
}
```

将代码编译为 JAR，仅依赖独立的 `dacp-plugin-api`（使用与服务器匹配的版本和 Scala 版本，以 `Provided` 方式依赖）。在 JAR 内创建以下服务注册文件：

```text
META-INF/services/dacp.DatasetProvider
```

文件内容为实现类全名：

```text
example.CsvFileProvider
```

将插件及其所需依赖 JAR 放入 `plugin_directory`（不要复制服务器已提供的 API 和依赖），重启时使用以下配置片段，并合并监听地址、TLS 和 URI 白名单：

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  measurements:
    provider: csv-file
    source:
      file: /private/measurements.csv
```

客户端读取 `/records`，不会获得 `source.file`。需要逐项映射的插件默认通过 `items` 接收参数，也可覆盖 `DatasetProvider.itemSettingsKey` 声明自己的配置键；解析后的映射统一由 `definition.items` 提供。`source` 可包含插件自定义的嵌套参数，插件必须自行校验类型、必填项和未知参数。

目录 provider 使用 `reader_overrides`，SQLDB 使用 `tables`，custom 使用 `items`。目录和 SQLDB 不接受 `items`。原生后端（例如未来的 MongoDB provider）可直接实现 `PublishedDataset` 和 `DatasetAccess`，提供列举、元数据、计数与流式读取，无需映射成文件或 SQL。完整接口和取消、资源释放要求见 [provider 开发指南](../../docs/dataset-providers.md)。MongoDB 尚未实现。

### 文件与结构化内容插件

文件插件实现 `FormatHandler`，在 `META-INF/services/dacp.FormatHandler` 注册。处理器自行校验私有 `options` 参数；例如 Excel 的设置为 `options: {sheet: Orders, header_row: 2}`。通用 `format` 和 `schema` 保持在读取配置顶层。

需要浏览文件内部逻辑对象时，实现 `StructuredFileHandler`，通过同一个格式服务文件注册。其 `openStructure` 返回 `DatasetAccess`，可直接流式读取内部对象，不要求提取为临时文件。文件根表现为带原始 bytes 视图的 folder，子对象显式声明 `records` 或 `bytes`。归档读取器可实现 `ContainerReader` 并在 `META-INF/services/dacp.ContainerReader` 注册，无需再修改核心格式注册表。

独立插件 API 位于 [dacp-plugin-api](../../dacp-plugin-api)，可运行的贡献示例和构建、安装步骤位于 [examples/plugins](../../examples/plugins/README.md)。`PluginChecks.listing` 提供可复用的元数据契约检查。服务端会关闭每次请求的访问句柄，并在关闭服务时释放 provider 资源。NetCDF、FITS 的具体解析插件及数组、图像新视图尚未实现；现有接口允许贡献者定义文件内部对象，不能静默把数组展开为记录。

## 开发与验证

DACP 优先采用插件机制扩展。新的数据集后端、文件格式和处理算子应通过实现公开扩展契约并安装插件加入，避免修改核心分发逻辑。内置实现与社区实现应使用相同契约，插件负责自己的参数校验与第三方依赖，客户端通过统一路径和能力访问数据。贡献方式、现有机制与科学数据扩展边界见 [插件贡献指南](../../docs/plugins.md)。

从项目根目录运行：

```bash
(cd dacp-server && sbt test)
.venv/bin/python -m pytest -q dacp-client/tests
(cd dacp-client-scala && sbt test)
```

服务启动并设置证书环境变量后，运行 SDK 与安全检查：

```bash
.venv/bin/python examples/client.py
.venv/bin/python examples/check_security.py
```

跨节点容器联调使用 `bash scripts/test-collaboration.sh`，需要 Docker。测试覆盖命名空间、格式读取、归档浏览、SQL 流、schema 校验、授权、流生命周期和任务协作。

协议定义位于 [proto/dacp.proto](../../proto/dacp.proto)，服务端位于 [dacp-server](../../dacp-server)，Python SDK 位于 [dacp-client](../../dacp-client)，Scala SDK 位于 [dacp-client-scala](../../dacp-client-scala)。


## 补充契约与联调验证

```bash
(cd dacp-operator-executors && sbt test)
(cd dacp-operator-repository && sbt test)
```

stage 服务端、构建所需运行时并准备示例证书后，使用 Docker 权限运行 `.venv/bin/python scripts/test-native-execution.py`，验证原生流、部分失败和取消。`scripts/test-source-submission.py` 需要启动 `examples/source-debug.yaml` 中的 DEBUG 服务，验证 Python/Scala heredoc。前置条件与测试端口见脚本及对应指南。

[技术参考](../reference.md)说明传输及配置。测试、示例与中英文文档须同步。本项目尚未发布，直接替换被废弃接口，不保留兼容别名或迁移回退。

## 算子审查材料

支持发布可选的源码包、仓库修订版本、许可证声明与文本、构建说明和 SBOM。公开页面提供下载并标注缺失信息。参见[源码与审查信息](../operator-provenance.md)。

## Union 开发

[Union 协议](../union.md) 定义证书身份、计算授权和直接流。`dacp-union-protocol` 提供共享传输工具，`dacp-server/UnionWorker` 复用插件数据访问及算子执行器，`dacp-union-server` 负责成员轮询、子任务准备和结果转发。运行 `cd dacp-union-server && sbt test` 验证真实 mTLS 的双节点契约。领域分析算子放在 `examples/union/operators`，不修改核心注册表。

普通 Flight 请求、Union 节点子任务和全局作业共用持久化执行记录。生命周期、原子写入及测试见 [执行记录](../execution-history.md)。

# 独立贡献插件示例

[English](README_en.md)

本项目仅依赖 `dacp-plugin-api` 编译，不依赖服务器实现，提供两个已注册的插件：

- `example-message`：原生 Dataset，提供 `/message` 和 `/summary`，通过 `source.message` 配置。
- `example-message-file`：处理 `.message` 文件的结构化文件处理器，直接提供 `/message` 和 `/summary` 子项，并支持私有配置 `options.prefix`。子项字节为包含所配前缀的 UTF-8 文本；文件根节点字节保持不变。

`/message` 提供 UTF-8 字节。`/summary` 提供一条 Arrow 记录，其 `characters` 列统计 Unicode 码点数量；显式记录数为 1。

此小型示例在内存中读取文本；生产处理器应对大型内容使用有界流式处理。

在项目根目录运行：

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd examples/plugins && sbt package)
(cd dacp-server && sbt stage)
mkdir -p plugins
cp examples/plugins/target/scala-2.13/dacp-example-plugins_2.13-0.1.0-SNAPSHOT.jar plugins/
```

将以下片段加入完整服务器配置（还需配置监听、TLS 和 URI 白名单）。相对路径基于服务器工作目录解析；本示例从 `dacp-server/` 启动：

```yaml
plugin_directory: ../plugins
datasets:
  greeting:
    provider: example-message
    source:
      message: Hello from a contributed dataset
  files:
    provider: mixed
    items:
      /document:
        file: /private/example.message
        options: {prefix: "Preview: "}
allowed_uri_prefixes:
  - dacp://localhost:8815/greeting
  - dacp://localhost:8815/files
```

使用该配置重启已通过 stage 构建的服务器，无需修改启动器、为插件重新构建服务器或修改客户端。上述 `stage` 命令用于首次构建服务器；后续插件变更只需重建插件 JAR 并重启服务器。

```python
from dacp import Source

with Source.open("dacp://localhost:8815/files") as ds:
    print(ds.list("/document"))
    with ds.open("/document/message") as stream:
        with stream.as_bytes() as reader:
            print(reader.read().decode("utf-8"))
```

注册文件位于 `src/main/resources/META-INF/services/`。文件及结构化文件处理器使用 `dacp.FormatHandler`，Dataset provider 使用 `dacp.DatasetProvider`。归档插件使用 `dacp.ContainerReader`，并自动获得格式条目。插件 JAR 和其私有依赖 JAR 应直接放在 `plugin_directory` 中，不扫描子目录。不要重复打包 DACP API 或服务器已提供的库。

服务器的 [PluginSpec](../../dacp-server/src/test/scala/dacp/PluginSpec.scala) 将这些源码编译进测试，并通过 Flight 验证，同时覆盖已注册的归档插件与生命周期检查。自己的测试可以运行 `PluginChecks.listing(access)`，检查递归列表、直接子项、路径与元数据。还应添加格式特定的流式处理、有界内存、错误、取消和资源释放测试。

动态目录示例 `example.JdbcCatalogProvider` 通过 `META-INF/services/dacp.DatasetCatalogProvider` 注册，查询数据库登记表并按需构建 Dataset。构建本工程后将 JAR 与 JDBC 驱动放到插件目录；配置及表结构见 [DatasetCatalog 指南](../../docs/dataset-catalogs.md)。

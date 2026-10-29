# 数据集、Item 与流访问

DACP 的访问链为 `DataCatalog → PublishedDataset → DatasetAccess → ItemAccess → stream`。`ItemAccess` 是可选的逐 Item 实现方式；需要自行管理多个 Item 的插件也可直接实现 `DatasetAccess`。

目录决定有哪些 Dataset，Provider 决定如何访问 Dataset；Item 是公开元数据，不包含本地文件、SQL 连接、MongoDB collection 或 S3 对象凭证。客户端始终使用 Dataset 名称、逻辑 Item 路径和视图。

| 层次 | 职责与生命周期 |
|---|---|
| `DataCatalog` | 动态列出、打开 Dataset；`open` 返回调用方负责关闭的 Dataset 租约 |
| `PublishedDataset` | 持有 Dataset 资源，`open(check)` 创建请求级访问对象 |
| `DatasetAccess` | 递归列举、子项列举、元数据、计数、扫描和读取；请求结束时关闭 |
| `ItemAccess` | 插件内部对象或游标的内容访问句柄；每次计数、扫描或读取后关闭 |
| Arrow 流 | `records` 使用声明的 Arrow schema；`bytes` 使用 `PluginSchemas.bytes` 分块封装 |

## 按 Item 实现后端

插件可以继承 `ItemDatasetAccess`，实现 `list()`、`list(path)`、`describe(path)` 和 `openItem(path)`。返回的 `ItemAccess` 实现 `count()` 和 `stream(view, allocator, listener, ready)`；可重写 `scan` 下推查询，否则使用公共的筛选、投影、limit 流式实现。

`describe` 必须显式声明插件原生支持的 `Item.views`，如 `Some(Seq("records"))` 或 `Some(Seq("bytes"))`。类型名不必注册为文件格式。未知计数返回 `None`；普通文件夹不开放内容视图。支持原始字节的结构根可以显式开放 `bytes`。

`ItemDatasetAccess` 验证逻辑路径和视图，在调用 `openItem` 前检查读取取消，并在成功、异常、读取中途取消时关闭返回的句柄。`openItem` 如果在返回前失败，应自行释放已分配的资源。`ItemAccess.stream` 同步执行：开始时调用 `listener.start(root)`，逐批调用 `putNext()`，成功后调用 `completed()`；在读取和发送前调用 `ready()`，并在 `finally` 中关闭 Arrow root、输入流或游标。不要将完整远程对象加载到内存，也不要关闭传入的 allocator。关闭 Item 不应关闭应用所有的共享连接池。

完整可编译示例见 [MessagePlugins.scala](../examples/plugins/src/main/scala/example/MessagePlugins.scala)：同一个原生 Dataset 提供 `/message` 的二进制流和 `/summary` 的记录流，无需本地文件或 JDBC。该示例为小数据演示；生产对象需有界分块。

## 注册与配置

实现 `DatasetProvider`，注册到 `META-INF/services/dacp.DatasetProvider`，将插件及私有依赖 JAR 安装到 `plugin_directory` 并重启。使用示例插件时：

```yaml
plugin_directory: /opt/dacp/plugins
datasets:
  messages:
    provider: example-message
    source:
      message: hello
```

动态 Catalog 可以直接返回该插件创建的 `PublishedDataset`，或通过 `CatalogContext.createDataset` 创建；不必经过 `PublishedItem`。`PublishedItem` 和 `FileDatasets.mixed` 是内置文件/JDBC 组合 Provider 的构建工具，不是所有后端的通用数据模型。

## MongoDB 与 S3 的接入位置

MongoDB Provider 可以将 collection 发布为逻辑 Item，由 `openItem` 创建游标并输出 records；必须明确字段、嵌套文档和类型映射，不能静默丢失结构。S3 Provider 可以将 prefix/key 映射成文件夹和 Item，由句柄打开对象响应体，分块输出 bytes；若另外支持 records，需明确解析器、schema 和扫描语义。两者的鉴权、分页、超时、取消和依赖由插件负责。客户端、任务与 Union 读取仍使用相同的 `DatasetAccess` 和 Flight 协议。

目前已实现这些扩展契约及独立插件示例；**MongoDB、S3 生产连接器尚未实现**。内置文件的格式识别、压缩包和结构浏览属于文件 Provider；SQL 查询优化属于 JDBC 访问实现。`DatasetStreams` 只分派公共访问接口，不识别具体存储类型。

契约测试覆盖独立插件的 Flight 列举与 records/bytes、原生记录扫描与计数，以及 Item 句柄在成功、失败、取消时的关闭。详见 [插件指南](plugins_zh.md)、[Provider 配置](dataset-providers_zh.md) 和 [动态 Catalog](dataset-catalogs_zh.md)。

## 通用 JSONL bytes 视图

插件仅声明 `records`、没有声明 `bytes` 时，DACP Server 会在统一的 Dataset 访问边界补充 `bytes` 视图。`list`、`list(path)` 和 `describe` 公布两种能力；Flight、S3、普通任务和 Union 节点输入都使用相同的访问对象。插件继续只实现原生 records 流，不需要增加 bytes 分支。`ItemByteViews.wrap` 是可供直接使用 DatasetAccess 的宿主应用复用的适配器。

转换规则：UTF-8、无 BOM、每条记录为一行 JSON 对象，以字段名作为键，每行以 `\n` 结尾；空记录流输出零字节。例如：

```jsonl
{"station":"北京","temperature":20.5,"quality":null}
{"station":"A","temperature":18.0,"quality":"ok"}
```

字符串正确转义，整数与十进制保留数值，null 保留，struct 为嵌套对象、list 为数组，不将数组展开成多条记录。二进制字段为 Base64 字符串；Java 时间/日期值为字符串，非有限浮点值为 `"NaN"`、`"Infinity"` 或 `"-Infinity"`。Arrow map 保留其 entries 数组结构。顶层字典编码解码为实际值；嵌套字典、不支持的值类型及重复字段名明确报错，不做有损转换。科学文件只有在插件已显式提供 records 时才适用此规则，不会自动展平文件结构。

插件已声明 bytes 时始终采用原有实现，即使该实现读取失败也不改用 JSONL。`format` 与 Arrow schema 保留原始记录的标识和结构，不伪装成原文件字节。派生 JSONL 的 `totalSizeBytes` 为未知，保留原有记录数；HEAD 或目录访问不会为求大小而扫描记录。S3 对未知大小的 Range 请求返回 501。

转换按 `byte_chunk` 有界分块，分块可以跨越一条记录或 UTF-8 字符，客户端应连接字节流后解码，不能逐块独立解析 JSON。每个输入批次后刷新输出，遵守背压与取消，不缓存整个记录集、不落盘。源读取或转换失败时传播失败，已输出内容视为不完整，不能当作完整成功结果。

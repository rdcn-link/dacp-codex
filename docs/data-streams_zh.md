# 类型化数据视图与流

DataCatalog 包含 Dataset，Dataset 通过逻辑路径暴露 Item。Item 可以可浏览、可读取，或者同时具备两种能力。文件容器可以同时提供原始字节和内部子项，子项可以是分组、表格、变量、图像 HDU 或序列。不能仅凭 folder 类型判断是否可读。

## 接口约定

`DatasetAccess.view(path, DataViewType[K,S])` 查找可选的 `DataView[K,S]`。使用类型化选择条件 `S` 和 `StreamContext` 打开视图，获得只能消费一次的 `DataStream[K]`。`consume` 同步调用一次 `start(descriptor)`，然后调用零次或多次 `accept(chunk)`。正常返回代表完成，抛出异常代表失败。空流仍须声明描述信息。消费结束自动关闭流，调用方也应在 `finally` 中关闭。

块是有界的数据批次，不是完整文件或单条记录。消费者返回后，生产者才能继续读取下一块，从而实现无需独立线程的背压。Arrow root、字典、字节数组和缓冲区仅在回调期间借用；需要保留时必须显式复制。allocator 由调用方管理。实现应在消费时打开游标、文件句柄，并在 `finally` 中释放，包括消费者抛出异常时。取消通过 `context.check` 协作实现，读取和输出前均应检查。关闭未消费的流不会打开资源；关闭正在消费的流会阻止后续回调，但不能保证中断任意第三方阻塞 I/O。

| 视图标记 | 数据块 | 选择条件 | 描述信息 |
|---|---|---|---|
| `DataViews.Bytes` | 带 offset/length 的 `ByteChunk` | `AllBytes` | 媒体类型 |
| `DataViews.Records` | 借用 Arrow root 和字典的 `RecordBatch` | `Scan` | Arrow schema 和字典 |
| `ScientificViews.Arrays` | `ArrayChunk` | `ArraySelection` | 维度、坐标、数值编码和属性 |
| `ScientificViews.Images` | `ImageTile` | `ImageSelection` | 图像大小、通道、编码和属性 |
| `ScientificViews.Sequences` | `SequenceChunk` | `SequenceSelection` | 字母表和属性 |

NetCDF、FITS、FASTA 解析插件及科学视图网络编码已实现，见 [xstream-data](../xstream-data/README_zh.md)。内置 read_item 可输出科学数据 Job 帧，下游算子需显式支持所选表示；Python 客户端可读取描述及数据块。

第三方插件可以定义自己的 `DataViewType[K,S]` 和 `DataDescriptor[K]`。生产者与消费者应共享同一个标记实例，按实例身份匹配，而不是根据同名字符串把无关块类型强制转换。Item 元信息中的视图名称仅用于声明能力，并不意味着已经安装传输编码器。

## 读取 records

```scala
val view = access.requireView("/observations", DataViews.Records)
val stream = view.open(Scan(), StreamContext(allocator, () => cancellation.check()))
try stream.consume(new DataConsumer[RecordBatch] {
  def start(d: DataDescriptor[RecordBatch]): Unit =
    println(d.asInstanceOf[RecordsDescriptor].schema)
  def accept(batch: RecordBatch): Unit =
    println(batch.root.getRowCount) // 返回之前完成消费
}) finally stream.close()
```

`Scan` 支持投影、过滤和全局行数限制。JDBC、Parquet 保留源端优化，其他读取器使用有界批次过滤。普通 records 读取保留字典；字典编码 records 的扫描仍明确不支持。`BatchDatasetAccess` / `BatchItemAccess` 是可选的 Arrow 生产工具，使用 `BatchReceiver`，无需实现 Flight 监听器。新实现也可直接实现 `DatasetAccess` / `ItemAccess` 并输出类型化块。

优先使用原始文件字节。SQL 表提供 Arrow IPC bytes。仅支持 records 的 Item，由 `ItemByteViews` 派生有界 UTF-8 JSONL bytes（`application/x-ndjson`），保留嵌套值并解码顶层字典。字节块无需与行或 UTF-8 字符对齐。错误发生后不再刷新缓冲中的不完整 JSON。传输层在边界编码流，本地执行和 S3 直接消费类型化块。本接口和视图转换不会隐式物化输出。

## 科学格式映射

| 格式 | 浏览结构与原生视图 | 约束 |
|---|---|---|
| CSV / JSONL | 文件 Item：records 和原始 bytes | 保留 schema、空值和嵌套值 |
| NetCDF | 分组、变量作为子 Item；变量提供 array | 保留秩、维度名、坐标引用、单位、缺失值及属性；按 origin/shape/stride 选择 |
| FITS | HDU 作为子 Item；表格 HDU 提供 records，图像 HDU 提供 array 或 image | 保留 HDU 身份、头信息、轴、缩放及坐标元数据，不自动展平 |
| FASTA | 原始 bytes、sequence 视图，可选序列子 Item | 块携带序列 ID、符号偏移及结束标记，不要求将整条染色体装入内存 |
| JPEG | 原始 bytes，可选解码 image 视图 | 保留图像大小、通道；解码必须显式进行 |

科学数值载荷使用明确的基础类型编码和字节序，块内按行优先连续存储。维度和块起点独立于载荷。坐标通过逻辑 Item 路径引用，不在每个块中重复复制。格式插件负责校验选择范围、缩放和格式专属属性。不支持的能力应该缺席，而不是返回空流。

## 扩展读取器

按照[插件文档](plugins_zh.md)，通过 `META-INF/services` 注册 `DatasetProvider` 或 `FormatHandler` / `StructuredFileHandler`，第三方依赖放在插件模块。结构化文件处理器返回内部逻辑对象的 DatasetAccess，不得把存储路径当成逻辑路径。配置放在插件自己的 dataset `source` / item 参数下，例如：

```yaml
datasets:
  climate:
    provider: my-netcdf
    source:
      root: /srv/climate
```

`my-netcdf` 是外部提供者示例，不是内置实现。插件自行校验 `root`、变量选择和解码选项。新增进程内视图无需修改核心注册表；新增网络表示则须另行定义编码器和传输能力协商。

契约测试应覆盖空流描述、有界块、借用缓冲区生命周期、选择与限制、消费者失败及取消时的资源释放、能力缺席、嵌套浏览，以及科学数据的维度、坐标、序列结构完整性。

## 科学视图网络表示（版本 1）

科学视图按有界数据块传输，不序列化完整文件。请求 ticket 包含视图名和 UTF-8 JSON 选择条件。数组使用 `{origin,shape,stride}`，省略表示完整变量；图像使用 `{x,y,width,height}`，省略表示完整图像；序列使用可选的 `{ids,offset,length}`。records 扫描条件不能与科学视图选择条件混用。

响应使用 `metadata: utf8`、`data: binary` 两列的 Arrow 信封，每个数据块一行。schema 元信息 `xstream.view` 标识表示类型，`xstream.descriptor` 保存 JSON 描述信息，空流也能读取描述。每块包含坐标或身份 JSON，以及原始载荷字节。数组和图像的数值缓冲区采用描述声明的字节序，按行优先连续存储；序列载荷使用 UTF-8，偏移按符号计数。元信息上限 1 MiB，单块上限 16 MiB。读取与传输传播错误和取消，不隐式物化。

`DataViewCodec` 通过 ServiceLoader 发现，与格式插件独立，负责类型化选择条件校验、描述信息及数据块编解码。服务与通用客户端根据注册的编码器分派，增加编码器无需修改核心注册表。NetCDF、FITS、FASTA 读取器和标准科学编码器作为独立 xstream-data 插件分发。数组、图像请求分别使用 `ArraySelection`、`ImageSelection`，未指定 section/region 表示完整对象，返回块始终具有明确坐标。

[设计器科学视图与 Job 帧契约](designer-scientific_zh.md)。

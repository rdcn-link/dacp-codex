# 添加格式处理器

服务器通过每个插件环境的 `Formats` 解析文件映射，`SqlAdapter` 单独处理 SQL。识别优先级为显式配置、复合归档后缀、签名匹配（最多八字节）、扩展名匹配、二进制回退。扩展名声明为小写，文件名匹配不区分大小写。ID 符合 `[a-z][a-z0-9-]*`，`sql` 保留。重复 ID 或扩展名导致注册表初始化失败。

在独立 JAR 中实现 `xstream.data.FormatHandler`，编译依赖独立的 `dacp-plugin-api`，范围为 `Provided`。Scala 直接实现 trait，Java 实现对应 JVM 接口；Scala trait 默认方法更适合通过 Scala 适配器复用。完整独立项目见 [examples/plugins](../examples/plugins/README_zh.md)。实现需要公开无参构造器。

```scala
package example

import dacp.{ByteSource, Records, FileOptions, FormatHandler}
import scala.jdk.CollectionConverters._

class LineProvider extends FormatHandler {
  val id = "lines"
  val extensions = Set("lines")
  override val records = true
  override def rows(source: ByteSource, options: FileOptions): Records.Rows =
    new Records.Rows {
      private val input = new java.io.BufferedReader(
        new java.io.InputStreamReader(source.open(() => ()), java.nio.charset.StandardCharsets.UTF_8))
      val names = Seq("value")
      val iterator = input.lines().iterator().asScala.map(line => Seq(line))
      def close(): Unit = input.close()
    }
}
```

在 JAR 中添加 `META-INF/services/xstream.data.FormatHandler`：

```text
example.LineProvider
```

将插件及私有依赖放入 `plugin_directory`，重启后由 ServiceLoader 发现。Java 实现需遵守 Scala 集合签名；可用小型 Scala 适配器调用 Java 解析库，Excel 对 Apache POI 的适配就是一个例子。

`rows` 提供有序列名、惰性迭代器和幂等 `close`。默认方法根据有界样本推断 schema、输出记录批次并计算显式计数。Parquet 等专用读取器可覆盖 `schema`、`stream`、`count`。列表中的 `cheapSchema`、`cheapCount` 必须避免全量扫描。识别、列表和读取对格式 ID 和公开能力的解释必须一致。`records = false` 只提供 bytes。

输入是可重新打开的 `ByteSource`，不要求本地 `Path`。源提供公开文件名、可选大小/版本、输入流和可选范围读取。源插件负责连接、认证及同一操作中的版本一致性。所有打开的流由调用方关闭。CSV/NDJSON 直接读流，Parquet 使用大小和范围读取；Excel 显式使用 `ByteSource.materialize` 创建必要的临时文件并随读取器关闭清理。`localPath` 只是一种可选优化。源不得将凭据或私有 URI 放入 `name`。

共享文件读取器不转换原始 bytes，打开 records 不得修改源。实现必须在成功、失败和取消时关闭解析器，并在每批读取前调用 `demand`。第三方错误应转换为固定公共消息的 `FormatFailure`，不包含文件名、工作表名、数据值或库诊断。SDK 将其转换为 `FORMAT_ERROR`。

`FileOptions` 包含私有 `format`、有序 Arrow columns 和由 YAML `options` 填充的 `parameters: Map[String,Object]`。覆盖 `validateOptions` 校验处理器专用键、类型和值，默认拒绝所有参数。Excel 自行处理 `options.sheet`、`options.header_row`，核心不含 Excel 校验分支。嵌套 YAML 保留 Java map/list。设置留在服务器端；公开 ticket 仅包含 Dataset、逻辑 Item 路径和视图。普通 CSV/NDJSON/Excel 列表不推断 schema，打开 records 时推断或校验配置列。folder 属性保持未知，不访问后代。

Excel 对 XLS 使用 HSSF 事件，对 XLSX 使用 SAX/XSSF，队列最多 16 行，解析事件之间检查取消。共享字符串字典和单行/单元格仍占内存。数值日期保留 Excel 序列值；只读公式缓存，不重新计算。多工作表必须配置选择。仅支持读取，不提供编辑或渲染。

## 归档读取器

`ContainerReader` 独立负责枚举子项、将条目暂存为可寻址文件。通过 `META-INF/services/xstream.data.ContainerReader` 注册，声明 `id`、`extensions` 和可选复合文件名匹配器；DACP 自动生成对应格式条目，不应另注册相同 ID 的格式。内置与外部读取器共用接口，拒绝重复 ID/扩展名和歧义匹配。

`NamespaceAdapter` 把映射归档发布为 folder，bytes 返回原文件，推导父目录，并将暂存条目交给格式读取器。folder 属性保持未知。嵌套归档保持二进制数据。GZIP 唯一子项从公开别名命名，不使用私有文件名。`.tar.gz`、`.tgz` 优先于普通扩展名识别。

ZIP 列表读取归档目录，不解压载荷；TAR 和压缩 TAR 顺序扫描，后者需解压；GZIP 列表只读头。读取条目时暂存一个文件，在成功、失败或取消时清理。TAR 暴露暂存内容前校验整个归档，包括目标之后的条目。扫描和解压遵守 demand/取消回调。

最多 10,000 个条目，单条解压后最多 512 MiB，声明总大小最多 2 GiB。TAR 的解压总量还限制为该大小加 16 MiB 元数据/填充。拒绝重复条目、无效路径和文件/父目录冲突。不支持 TAR 链接、稀疏文件和特殊条目。归档根拒绝 records，数据子项通过常规格式注册表声明视图。

## 原生结构化文件

实现 `StructuredFileHandler` 并在格式服务文件注册。`openStructure(source, options, context, check)` 返回以 `/` 为根的请求级 `DatasetAccess`，可以直接暴露逻辑对象，无需提取文件。公开绝对路径相对结构根，服务器加上发布文件别名前缀。实现递归 `list()`、直接子项 `list(path)`、`describe`、`count` 和类型化 `view`；视图打开同步 `DataStream`，空流也必须声明描述信息。子 Item 使用无文件处理器的原生格式时，必须明确声明视图；records schema 应匹配 records 视图；非表格的科学视图使用自己的类型化描述信息，不伪造 Arrow 表格 schema。核心在成功和失败后关闭句柄。发现时遵守 `check`，读写数据块前遵守 `StreamContext.check`；取消后也要释放文件句柄。

文件根是带原文件 bytes 的 folder，属性未知；根元数据不打开结构或计算后代。嵌套 folder 属性同样未知。子对象 bytes 的语义由处理器说明，根 bytes 始终保留原文件。普通列表不得计算昂贵计数，启动计数跳过结构化根。

使用 `PluginChecks.listing(access)` 检查元数据契约。[PluginSpec](../dacp-server/src/test/scala/dacp/PluginSpec.scala) 展示流、取消、私有错误和清理测试。NetCDF/FITS 可通过该接口暴露组、变量和 HDU，但尚不包含解析器及数组/图像视图。记录投影必须明确设计，不能静默展开数组。

## 记录扫描

`FormatHandler.scan` 为所有 records 处理器提供有界批次过滤、投影和到达限制后的提前终止。可覆盖以优化源读取；directory、mixed 及归档子项使用同一契约。原生结构化子项使用 `DataViews.Records`。见[记录扫描契约](records-scans_zh.md#插件实现契约)。

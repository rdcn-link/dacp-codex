# DACP Scala SDK

[English](README_en.md)

独立的 Scala 2.13 SDK，通过 DACP 服务读取 Dataset，使用 TLS/Token 认证。需要 JDK 17 和 sbt 1.10.x。使用共享的 [protobuf 定义](../proto/dacp.proto)，不依赖 DACP 服务器项目。

在本目录构建并发布到本地 Ivy 仓库：

```bash
sbt test publishLocal
```

在 Scala 2.13 应用的 `build.sbt` 中添加：

```scala
libraryDependencies += "io.github.rdcn-link" %% "dacp-client-scala" % "0.1.0"
```

执行 `publishLocal` 后即可使用该依赖；它尚未发布到 Maven Central。按照本 Arrow 版本的要求，运行应用 JVM 时需添加 `--add-opens=java.base/java.nio=ALL-UNNAMED` 和 `-Dio.netty.tryReflectionSetAccessible=true`。sbt 应用配置如下：

```scala
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true")
```

通过 `DACP_CA`、`DACP_CERT`、`DACP_KEY` 配置 PEM 文件路径，或使用 `TlsConfig` 显式传递路径。服务器证书验证保持启用。未指定 CA 时使用默认信任根；客户端证书和密钥必须成对提供。普通连接使用 Token，无需客户端证书；通过 `TlsConfig(tokenFile = Some(Path.of("/protected/alice.token")))` 或 `DACP_TOKEN_FILE` 提供。

```scala
import dacp.client.{Source, TlsConfig}
import java.nio.file.Path
import scala.util.Using

val tls = TlsConfig(
  ca = Some(Path.of("examples/certs/ca.crt")),
  tokenFile = Some(Path.of("/protected/alice.token")))

Using.resource(Source.open("dacp://localhost:8815/data1", tls)) { ds =>
  ds.list("/").foreach(item => println(s"${item.path}: ${item.kind}, ${item.views}"))
  println(ds.list("/reports"))

  Using.resource(ds.open("/reports/orders")) { item =>
    Using.resource(item.asRecords()) { records =>
      println(records.schema)
      while (records.next()) {
        val batch = records.root
        println(batch.contentToTSVString())
      }
    }
  }

  Using.resource(ds.open("/image")) { item =>
    Using.resource(item.asBytes()) { bytes =>
      println(bytes.readNBytes(8).map(b => f"${b & 0xff}%02x").mkString(" "))
    }
  }
}
```

直接访问 Item URI：

```scala
Using.resource(Source.openItem("dacp://localhost:8815/data1/image", tls)) { item =>
  Using.resource(item.asBytes()) { bytes =>
    println(bytes.readNBytes(8).length)
  }
}
```

`Source.open` 接收 Dataset URI 并返回 `Dataset`；`Source.openItem` 接收 Item URI 并返回 `ItemStream`。`Dataset.list()` 递归返回所有 Item，包括文件夹和数据。`Dataset.list("/")` 列出根节点子项，`Dataset.list("/reports")` 只列出该文件夹的直接子项。每个 `Item` 包含绝对逻辑 `path`、`kind`（`folder` 或 `data`）、`format`、支持的 `views` 和可选 Arrow schema。路径不标识私有文件或 SQL 表名。使用 `Dataset.open("/reports/orders")` 访问数据；普通文件夹仅支持列举，而归档和压缩文件夹还提供 bytes。相对路径会被拒绝。原始 CSV/NDJSON/Excel 的列表 schema 可能缺失，可通过 `asRecords().schema` 获取记录 schema。服务器启动日志提供已配置的 Dataset 名称，协议尚未实现 Dataset 发现。

Item 统计信息均为可选：`lastModifiedTimeMs: Option[Long]`（UTC Unix 毫秒）、`lastModifiedTime: Option[Instant]`、`totalSizeBytes: Option[Long]` 和 `recordCount: Option[Long]`。`Some(0L)` 表示已知为零，`None` 表示未知或不适用。文件元数据提供时间和大小；CSV/NDJSON/Excel 浏览时不统计记录，`ds.count(path)` 才显式扫描。Parquet 使用文件尾计数。SQL 浏览时计数、时间和字节大小均未知；`ds.count("/reports/orders")` 显式请求精确的 `COUNT(*)`。所有文件夹属性始终为 `None`，包括空文件夹；文件夹元数据不会访问子 Item。二进制数据的记录数未知。

列表操作避免扫描记录。显式计数可能扫描文件或 SQL 表，开销较大。SQL 计数遵守配置的语句超时，客户端 RPC 截止时间同样有效。统计值是观测结果，与后续读取不构成原子快照。

`asRecords()` 返回可关闭的 `RecordStream`。调用 `next()` 加载批次后读取 `root`。Arrow 在后续调用中复用 root，因此必须在前进前消费或复制内容，且不得在读取器关闭后保留它。CSV、NDJSON、Excel、带 Arrow schema 元数据的 DACP 生成 Parquet，以及 SQL 表均支持 records 视图。

`asBytes()` 返回 Java `InputStream`。文件 Item 返回原始文件字节；SQL 表返回 Arrow IPC 流字节。读取按块增量消费，不收集整个 Item。支持标准的 `readNBytes` 和 `transferTo`；`readAllBytes` 会将整个 Item 收集到内存中，应仅用于小型资源。

对 Dataset、Item 句柄和读取器使用 `Using.resource`，包括提前停止的场景。关闭读取器会取消未完成读取并释放 Arrow 缓冲区；关闭 Item 会关闭其读取器；关闭 Dataset 会关闭其 Item、Flight 连接和分配器。通过 Dataset 打开的 Item 共享其连接。直接访问 Item 时独占私有 Dataset 连接，并随 Item 一起关闭。这些对象按单线程使用设计。

每次列举、Item 查找或内容读取默认具有 30 秒截止时间。对于长流，在 `Source.open` 或 `Source.openItem` 上设置更大的 `timeoutSeconds`；截止时间作用于整个 Flight RPC，而不仅是建连。失败后的读取器不可继续使用，应关闭并重新打开资源，从头重试。没有自动重试或断点续读。

测试启动进程内 Flight 服务器，检查元数据、记录、字节块边界、部分读取、EOF、失败行为、URI 验证、取消和资源清理。若还需测试本地运行中的示例服务器，在本目录运行：

```bash
DACP_CA=../examples/certs/ca.crt \
DACP_CERT=../examples/certs/client.crt \
DACP_KEY=../examples/certs/client.key \
DACP_SCALA_TEST_URI=dacp://localhost:8815/data1 \
sbt test
```

传输失败以 `DacpException` 返回，包含稳定的 `code` 和可读消息，不含底层传输描述或异常原因。请处理 `NOT_FOUND`、`ACCESS_DENIED`、`UNAVAILABLE`、`TIMEOUT` 等 DACP 错误码，而非传输异常。流失败后不可继续读取，应重新打开 Item 重试。

格式描述源数据：SQL 为 `sql`，`.jsonl`/`.ndjson` 为 `ndjson`，`.xls`/`.xlsx` 为 `excel`。普通 `.json` 仅支持 bytes，除非服务器将其配置为 `ndjson`。请依据各 Item 公布的 `views` 选择读取方法；新增格式处理器无需修改 SDK。

ZIP 归档显示为 `kind=folder`、`format=zip`，提供 `bytes` 视图。使用 `ds.list("/archive")` 浏览条目，使用 `ds.open("/archive/reports/orders.csv").asRecords()` 读取 CSV 条目，使用 `ds.open("/archive").asBytes()` 下载归档。打开文件夹前检查公布的视图；普通文件夹没有流。嵌套归档保持为二进制 data Item。

TAR（`tar`）、GZIP（`gzip`）及压缩 TAR（`tar-gzip`、`.tar.gz` / `.tgz`）与 ZIP 使用相同的文件夹模型。默认示例可浏览 `/bundle` 或 `/bundle_gzip`，通过 `/orders.csv.gz/orders.csv` 读取解压后的 CSV 记录，通过 `/orders.csv.gz` 的 `asBytes()` 读取原始压缩文件。GZIP 子项名称来自公开别名；不带 `.gz` 后缀的别名使用 `content`。TAR 顺序扫描，压缩 TAR 扫描需要解压，因此大型归档可能延迟列举或打开。嵌套容器保持为二进制 Item。

Dataset 在服务器端构建：`directory`、`sqldb` 和 `mixed` provider 提供相同 SDK API。默认服务器的 `documents` Dataset 自动发现 `examples/data`；连接 `dacp://localhost:8815/documents` 后使用 `list("/")` 或 `open("/nested/orders.csv")`。目录变更会出现在后续请求中。公开表别名和私有数据源绑定保留在服务器端。

## 过滤、投影与限制

```scala
import dacp.client.Expressions.col

Using.resource(ds.open("/reports/orders",
  predicate = Some((col("id") > 1) && col("amount").isNotNull),
  columns = Some(Seq("amount")), limit = Some(100L))) { item =>
  Using.resource(item.asRecords()) { records =>
    while (records.next()) println(records.root.contentToTSVString())
  }
}
```

在上述 Dataset 资源作用域内使用已打开的 `ds`。相等使用 `===`，不等使用 `=!=`，集合成员检查使用 `.isin(Seq(...))`，空值检查使用 `.isNull`/`.isNotNull`。服务器先过滤，再投影并应用全局限制；bytes 不接受查询选项。参见[共享语义与示例](../docs/records-scans.md)。

## 流式作业

```scala
import dacp.client.Job
import java.nio.file.{Files, Path}
import scala.util.Using

Using.resource(Job.run("dacp://localhost:8815",
  Files.readString(Path.of("examples/multi-node/job.yaml")), tls,
  timeoutSeconds = 180)) { events =>
  events.foreach { event =>
    if (event.kind == "data") println(s"${event.port}: ${event.data.length} bytes")
  }
}
```

`JobStream` 是实现 `AutoCloseable` 的迭代器。即使提前停止也应使用资源作用域，关闭会取消剩余作业。迭代结束或失败会自动释放资源。交付数据后出错表示结果不完整。输出端口元数据包含表示、格式和记录 schema。事件负载数组在前进或关闭流后仍有效。

对于 records 数据事件，使用自己的 Arrow 分配器调用 `event.records(allocator)`，并在资源作用域内关闭返回的读取器。其单个记录批次借用于该读取器，在读取器前进或关闭前有效。源 Dataset 仍可浏览；作业结果为流，不能重新打开。

## Union 客户端

`dacp.client.Union` 支持向独立 X 提交作业，分别读取 `results()` 和 `events()`，查询 `status()` 或 `cancel()`。执行和流均用 `Using.resource` 关闭。完整代码及认证配置见 [Union 服务器](../dacp-union-server/README.md)。

# 在用户程序中启动 DACP Server

[English](embedded-server.md) · [动态目录](dataset-catalogs_zh.md)

`dacp.DacpServer` 是可嵌入的 Scala/JVM API。命令行入口 `dacp.Main` 也使用同一实现，因此两种方式都包含 Flight TLS / Token、普通任务、可选 Union worker、执行记录和相同的授权检查。

## 启动与停机

已有服务器配置时：

```scala
import dacp.DacpServer

val server = DacpServer.builder()
  .configFile("/etc/dacp/server.yaml")
  .build()

try {
  server.start()             // 监听启动后返回，用户程序可继续运行。
  server.awaitTermination()  // 可选：让当前线程等待停机。
} finally {
  server.close()
}
```

`configFile` 接受 `String` 或 `Path`。也可使用 `.config(config)` 传入新建的 `dacp.Config`。构建器只能 `build()` 一次；服务器只能启动一次，关闭后需构建新的实例。`boundPort` 和 `location` 在启动后提供实际 Flight 监听信息。

`close()` 可重复调用。启动失败时自动清理已分配的服务、目录、执行记录和 Arrow 内存；即使某个关闭操作抛出异常，也继续关闭其余资源。`build()` 成功后，服务器拥有传入的 Config 和目录实例；即使尚未启动，也应关闭服务器。不要在多个服务器之间共享一个 Config 或同一个可关闭的目录实例。

嵌入 API 不安装 JVM 退出钩子，不关闭进程级共享插件加载器。宿主程序负责决定何时 `close()`。命令行入口自行安装退出钩子。插件目录及静态扩展注册表目前是 JVM 级配置，同进程的多个实例不能使用不同插件安装目录；每个实例的数据目录、监听端口和执行历史目录仍应独立。

## 直接传入用户目录

```scala
val catalog = new MyDatabaseCatalog(database)
val server = DacpServer.builder()
  .configFile("/etc/dacp/server.yaml")
  .catalog(catalog)
  .build()

server.start()
// 用户程序继续运行，管理自己的数据库或更新目录。
// 停机时调用 server.close()。
```

这里的 `MyDatabaseCatalog` 是用户实现的 `DatasetCatalog`。直接传入实例时无需 ServiceLoader 注册；它替代文件中的 `datasets` 或 `dataset_catalog`，不会创建被替代的 provider。使用 `.config(config)` 时必须在该 Config 的目录初始化之前注入，不能先调用 `Config.load` 初始化默认目录，再替换它；需要配置文件时使用 `.configFile(...).catalog(...)`。

每次 `catalog.open` 返回调用方拥有的 `PublishedDataset` 租约，服务器负责关闭访问句柄和租约。服务器停机关闭目录实例；目录不能关闭由宿主程序管理的共享数据库连接池。完整契约见[动态目录](dataset-catalogs_zh.md)。

## 可运行示例与工程依赖

[完整示例](../examples/embedded-server/README_zh.md)在同一 JVM 中维护并发注册表，在服务器启动后加入 `reports` Dataset，并通过普通 Flight 接口发布 CSV 数据。

当前通过源码工程依赖接入，项目使用 Scala 2.13.16 / JDK 17。独立应用的 sbt 配置可参考示例：

```scala
ThisBuild / scalaVersion := "2.13.16"
lazy val server = RootProject(file("/path/to/dacp-codex/dacp-server"))
lazy val app = (project in file(".")).dependsOn(server)
fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "-Dio.netty.tryReflectionSetAccessible=true"
)
```

节点仍需有效 TLS 证书与授权配置。只提供数据浏览和读取无需 Docker；执行算子时才需要对应容器环境。

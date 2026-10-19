# 自定义 Scala 算子

构建前，请按 [SDK 说明](../../../dacp-operator-sdk/README.md)将 plugin-api 和 operator-sdk 发布到本地依赖库。新增 [SimpleIdentity](src/main/scala/example/SimpleIdentity.scala) 使用模板；原 ScalaIdentity 展示底层协议。

[English](README_en.md)

此独立示例实现 `scala_identity@1.0`，无需导入或重新构建 DACP 服务器。它生成薄 JAR，并复用 `dacp/operator-jvm:1.1` 中的 Scala、Arrow、Jackson 和 JVM。算子保留原始输入字节；可以将批次处理代码替换为自己的变换逻辑。

示例文件：

- [完整 Scala 源码](src/main/scala/example/ScalaIdentity.scala)
- [薄 JAR 的 sbt 构建配置](build.sbt)
- [算子定义](operators/scala_identity/1.0/operator.yaml)
- [编写与上传的逐步指南](../../../docs/operator-authoring.md)

## 构建与发布

在项目根目录运行，需要 JDK 17、sbt 和 Docker 访问权限：

```bash
# 首次构建共享环境，已安装时可跳过。
scripts/build-operator-image.sh
(cd examples/operators/scala && sbt package)
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators examples/operators/scala/operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --port 8820
```

配置执行节点：

```yaml
remote_operator_library_urls:
  - http://localhost:8820
```

在已连接的 shell 中运行：

```text
run scala_identity@1.0 --input source=/events
```

算子也支持本地执行：从 `dacp-server` 启动时，设置 `local_operator_library: ../examples/operators/scala/operators`，并提前构建 JAR 和安装运行时镜像。

## 入口点与流契约

JVM 启动器向 `example.ScalaIdentity.main` 传递 `operator /input /input/params.json`。从 `/input/manifest.json` 读取具名输入的元数据（`port`、`file`、`representation`、`format`）。示例接收一个名为 `source` 的 bytes 端口，用仅含二进制 `data` 列的 Arrow IPC 流表示。读取到 IPC EOS 时停止，包括 FIFO 输入。

将 `result` 端口的 start/data/end 帧写到标准输出，诊断信息写到标准错误。Shell 增量消费输出，不会发布保存后的 Dataset。完整源码演示了帧编码与资源清理。参见[执行契约](../../../docs/operator-authoring.md)。

## 缩小制品体积

[build.sbt](build.sbt) 将运行时依赖标记为 `Provided`，避免将它们打入算子 JAR。Scala 和依赖版本应与共享运行环境匹配。如果算子需要额外库，可以构建新共享运行环境并将该库放入 `/opt/lib`；仅在合适时将私有库打入自包含 JAR。仅更新代码时复用已有运行环境摘要。

[operator.yaml](operators/scala_identity/1.0/operator.yaml) 选择运行时镜像、执行器、主类和 JAR 路径。为其他算子发布不同的名称或入口点。多个算子可以引用同一 JAR 中的不同主类，无需修改核心注册表、重新构建服务器或修改客户端。

要在真实容器中验证此 JAR 及两类 Python 制品，请先按 [Python 示例](../python/README.md)构建 Python 环境，再从项目根目录运行：

```bash
(cd dacp-server && DACP_TEST_OPERATOR_PACKAGES=1 sbt 'testOnly dacp.OperatorPackageSpec')
```

执行器插件、验证、缓存、资源限制和平台限制见[算子打包](../../../docs/operator-packaging.md)。

完整源码示例、wheel 打包、远程文件传输、管理员发布和执行步骤见[编写与上传自定义算子](../../../docs/operator-authoring.md)。

# DACP 算子运行时

[English](README.md)

`format_convert@1.1` 和 `identity@1.1` 的独立 Scala 实现，不含数据服务器、Docker 客户端、SQL 驱动、Excel 读取器或任务协调器。两个算子共享薄 JAR 和 JVM/Arrow/Parquet 依赖环境。服务器仅在测试中依赖本模块。

在项目根目录、Linux 环境下构建，需要含 `jlink` 的 JDK 17、sbt、Python 3、`ldd` 和本地 Docker Engine 访问权限：

```bash
scripts/build-operator-image.sh
.venv/bin/python scripts/test-operator-image.py
```

构建输出：

- `dacp/operator-jvm:1.1`：裁剪 JVM、原生库及 `/opt/lib` 下的共享依赖，不含算子实现 JAR。
- `dacp-operator-runtime/target/universal/stage/lib/default.dacp-operator-runtime-0.1.0-SNAPSHOT.jar`：独立发布的小代码包，两份初始定义通过 `executor: jvm` 共享。
- `dacp/format-convert:1.1`：可选完整容器，在共享环境上增加独立代码层。

可选首参数选择完整镜像标签，例如 `scripts/build-operator-image.sh dacp/format-convert:test`；使用 `.venv/bin/python scripts/test-operator-image.py --image dacp/format-convert:test` 验证。

镜像基于 `scratch`，不拉取基础镜像，不含 JDK、sbt、shell 或服务器发行包。构建主机须与 Docker Engine 的 Linux 架构一致（x86_64/aarch64）。首次依赖下载需访问 Maven。Snappy/Zstd 原生库在构建时提取，避免在 `/tmp` 解包可执行库；环境通过 `JAVA_TOOL_OPTIONS` 提供原生压缩库路径。

入口接收 `operator /input /input/params.json`，`DACP_OPERATOR` 选择精确版本。算子实现 [Arrow IPC 契约](../docs/reference_zh.md#任务执行与算子)。Parquet 输出使用 Java 文件 I/O，无 Hadoop shell 命令。转换支持 CSV/NDJSON、推断或显式 schema，以及 none/Snappy/Zstd 压缩。镜像测试在禁用网络、只读根文件系统和临时 `/tmp` 下验证两种格式、全部压缩模式、schema 元数据、Arrow 解码及 identity 字节保留。

仓库打包后，在项目根目录发布：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
```

已测试的 Linux x86_64 构建中，共享运行时归档为 116,087,296 字节，初始代码包为 37,090 字节。两者均按摘要只存一份，不再为每个算子存完整归档。依赖、JDK 或架构变化会影响测量。版本不可覆盖，不提供不受支持版本的别名。

详见 [算子打包](../docs/operator-packaging_zh.md)、[Scala 贡献示例](../examples/operators/scala/README_zh.md) 和 [Python 示例](../examples/operators/python/README_zh.md)。

输出通过 stdout 的 start/data/end 帧传输，不创建输出数据集；Parquet 所需的临时文件仅位于容器 `/tmp`。详见[流契约](../docs/operator-streams_zh.md)。

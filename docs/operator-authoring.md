# 编写与发布用户自定义算子

自定义算子由独立程序与仓库清单组成。新增算子无需实现服务端插件或重建服务器。可以使用薄 Scala/Java JAR、Python 源码或 wheel，并在多个算子之间共享依赖环境。

**当前发布需要仓库管理员的 CLI 权限。** 公开网站支持搜索、查看与下载，没有 HTTP 上传接口或自助贡献者账户。这里的“上传”指把代码传至仓库主机，由授权管理员运行 `publish` 或 `seed`。

除特别说明外，命令均在项目根目录执行。本地发布需要 JDK 17、sbt、Linux、Docker Engine 权限及首次依赖下载。提供已发布制品下载无需 Docker。当前清单格式应使用新的空开发目录。

## 1. 执行契约

执行器启动主类、脚本或模块，并传入：

```text
operator /input /output /input/params.json
DACP_OPERATOR=<name>@<version>
```

- 读取 `/input/params.json` 并校验支持的参数。identity 示例使用 `{}`，不修改数据值。
- `/input/manifest.json` 是数组，例如 `[{"file":"0.arrow","path":"/events","format":"csv"}]`。`file` 相对 `/input`；公开 `path` 不一定是物理文件名。
- 每个输入是只有一列 binary `data` 的 Arrow IPC stream。值是原始文件的字节块，**不是自动解码的记录**。CSV 转换应先把这些块作为 CSV 字节流解析；块边界不一定是记录或字符边界。
- 有界处理每个 batch 并关闭资源。单输入任务可能使用 FIFO，应在 Arrow EOS 停止，不能等待操作系统 EOF；多个任务输入在执行前落地。
- 将每个结果包装为 `/output` 下的 binary Arrow IPC stream，再写 `_output.json`：

```json
[{"file":"_stream-0.arrow","path":"part-0.csv","format":"csv"}]
```

`file` 是 IPC 文件名，`path` 是最终解码文件名，`format` 描述输出字节格式。仅使用单文件名，不使用路径或符号链接；最终 `path` 不能以下划线开头。输出标签必须与实际字节匹配。仅在进程成功退出后，服务器才解码和发布；抛异常或非零退出表示失败。

容器无网络，根目录、代码和输入只读，`/output` 与临时 `/tmp` 可写。依赖必须已存在于运行时。当前限制为 1 GiB 内存、128 个 PID、512 MiB 临时空间及 30 分钟执行期限。运行时镜像须匹配执行平台。

## 2. Scala 示例：保留输入字节

完整项目见 [examples/operators/scala](../examples/operators/scala/README.md)。可从下方实现开始，将 batch 复制逻辑替换为自己的处理。代码使用 Arrow/Jackson，不依赖服务器实现。

`src/main/scala/example/ScalaIdentity.scala`：

```scala
package example

import java.nio.file.{Files, Path, StandardOpenOption}
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.ipc.{ArrowStreamReader, ArrowStreamWriter}
import scala.jdk.CollectionConverters._

/** Independently compiled operator: no dependency on DACP server classes. */
object ScalaIdentity {
  def main(args: Array[String]): Unit = {
    require(args.length == 4 && args(0) == "operator", "expected operator INPUT OUTPUT PARAMS")
    val input = Path.of(args(1)); val output = Path.of(args(2))
    val json = new ObjectMapper()
    val params = json.readTree(Path.of(args(3)).toFile)
    require(params.isObject && params.size() == 0, "scala_identity accepts no parameters")
    val manifest = json.readTree(input.resolve("manifest.json").toFile)
    val result = json.createArrayNode()
    val allocator = new RootAllocator(256L * 1024 * 1024)
    try manifest.elements.asScala.zipWithIndex.foreach { case (item, index) =>
      val format = item.path("format").asText
      require(format.matches("[a-z][a-z0-9_-]*"), "invalid input format")
      val filename = item.path("file").asText
      require(filename.nonEmpty && !filename.contains("/") && !filename.contains("\\"), "invalid input filename")
      val streamName = s"_stream-$index.arrow"
      val reader = new ArrowStreamReader(Files.newInputStream(input.resolve(filename)), allocator)
      try {
        val root = reader.getVectorSchemaRoot
        require(root.getSchema.getFields.size() == 1 && root.getVector(0).isInstanceOf[org.apache.arrow.vector.VarBinaryVector] &&
          root.getSchema.getFields.get(0).getName == "data", "expected Arrow bytes view")
        val writer = new ArrowStreamWriter(root, null,
          Files.newOutputStream(output.resolve(streamName), StandardOpenOption.CREATE_NEW))
        try {
          writer.start()
          // Process one bounded batch at a time. IPC EOS also works with DACP FIFO inputs.
          while (reader.loadNextBatch()) writer.writeBatch()
          writer.end()
        } finally writer.close()
      } finally reader.close()
      val extension = Map("excel" -> "xlsx", "sql" -> "bin").getOrElse(format, format)
      result.addObject().put("file", streamName).put("path", s"part-$index.$extension").put("format", format)
    } finally allocator.close()
    Files.write(output.resolve("_output.json"), json.writeValueAsBytes(result))
  }
}
```

`build.sbt`：

```scala
name := "scala-identity"
organization := "example"
version := "1.0"
scalaVersion := "2.13.16"
// Supplied by dacp/operator-jvm:1.1; keep them out of the thin operator JAR.
libraryDependencies ++= Seq(
  "org.apache.arrow" % "arrow-vector" % "18.3.0" % Provided,
  "org.apache.arrow" % "arrow-memory-netty" % "18.3.0" % Provided,
  "com.fasterxml.jackson.core" % "jackson-databind" % "2.18.3" % Provided)
Compile / mainClass := Some("example.ScalaIdentity")
```

共享环境提供的依赖标记为 `Provided`，避免打进 fat JAR。`dacp/operator-jvm:1.1` 提供兼容的 Scala 2.13、Arrow 18.3.0 和 Jackson。需要不同或额外依赖时构建匹配的共享运行时，不在任务期间下载。

先构建一次共享 JVM 环境和仓库启动程序：

```bash
# From the dacp-codex project root.
scripts/build-operator-image.sh
(cd dacp-operator-repository && sbt stage)
```

在仓库主机构建并发布小 JAR：

```bash
(cd examples/operators/scala && sbt package)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --root .operator-catalog --image dacp/operator-jvm:1.1 \
  --executor jvm --entrypoint example.ScalaIdentity \
  --artifact examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  --description "Copy input bytes using a small Scala operator"
```

已测试的示例 JAR 约 5.5 KB。运行时单独计量，每个节点对同一摘要只下载一次。

## 3. Python 示例：源码或 wheel

可运行源码见 [identity.py](../examples/operators/python/identity_python/1.0/identity.py)：

```python
"""DACP byte-preserving operator. Dependencies live in the shared runtime image."""
import json
from pathlib import Path
import re
import sys

import pyarrow as pa


def main():
    if len(sys.argv) != 5 or sys.argv[1] != "operator":
        raise ValueError("expected operator INPUT OUTPUT PARAMS")
    source, target = map(Path, sys.argv[2:4])
    params = json.loads(Path(sys.argv[4]).read_text())
    if not isinstance(params, dict) or params:
        raise ValueError("identity_python accepts no parameters")
    result = []
    schema = pa.schema([("data", pa.binary())])
    for index, item in enumerate(json.loads((source / "manifest.json").read_text())):
        if not re.fullmatch(r"[a-z][a-z0-9_-]*", item["format"]):
            raise ValueError("invalid format")
        name = f"_stream-{index}.arrow"
        with (source / item["file"]).open("rb") as raw:
            with pa.ipc.open_stream(raw) as reader:
                if reader.schema != schema:
                    raise ValueError("expected Arrow bytes schema")
                with (target / name).open("xb") as sink:
                    with pa.ipc.new_stream(sink, schema) as writer:
                        for batch in reader:
                            writer.write_batch(batch)
        # IPC EOS terminates FIFO reads; do not wait for operating-system EOF.
        extension = {"excel": "xlsx", "sql": "bin"}.get(item["format"], item["format"])
        result.append({"file": name, "path": f"part-{index}.{extension}", "format": item["format"]})
    (target / "_output.json").write_text(json.dumps(result))


if __name__ == "__main__":
    main()
```

运行时 [Dockerfile](../examples/operators/python/Dockerfile) 使用 Python 3.12 与 `pyarrow==23.0.1`。构建一次并发布源码：

```bash
docker build -t dacp/operator-python:1.0 examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_python@1.0 \
  --root .operator-catalog --image dacp/operator-python:1.0 \
  --executor python --entrypoint identity \
  --artifact examples/operators/python/identity_python/1.0/identity.py \
  --description "Copy input bytes using Python"
```

多个模块/算子或含编译扩展时使用 wheel。项目提供的 [pyproject.toml](../examples/operators/python/pyproject.toml) 将同一脚本打包为 `identity` 模块：

```toml
[build-system]
requires = ["setuptools>=68", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "dacp-example-ops"
version = "1.0"
description = "Small DACP operator code using a shared Python/Arrow runtime"
requires-python = ">=3.12"
dependencies = ["pyarrow==23.0.1"]

[tool.setuptools]
py-modules = ["identity"]
package-dir = {"" = "identity_python/1.0"}
```

```bash
# Requires setuptools and wheel in the selected build environment.
.venv/bin/python -m pip wheel --no-deps --no-build-isolation \
  --wheel-dir examples/operators/python/dist ./examples/operators/python

dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 \
  --root .operator-catalog --image dacp/operator-python:1.0 \
  --executor python --entrypoint identity \
  --artifact examples/operators/python/dist/dacp_example_ops-1.0-py3-none-any.whl
```

示例 wheel 约 2 KB。`dependencies` 元数据记录运行时依赖要求，但执行器以 `--no-index --no-deps` 离线安装，故运行时须已有 PyArrow。源码也需要 `entrypoint` 元数据，实际运行脚本本身；wheel 根据此字段选择模块。模块应在 `if __name__ == "__main__"` 中调用处理函数。

一个 JAR 的不同主类或一个 wheel 的不同模块，可以发布为不同算子名称并共享同一代码与运行时；发布时分别设置名称和入口。

## 4. 可选定义文件与批量发布

也可使用 `<definitions>/<name>/<version>/operator.yaml`：

```yaml
name: scala_identity
version: "1.0"
description: Copy bytes with Scala
image: dacp/operator-jvm:1.1
executor: jvm
entrypoint: example.ScalaIdentity
artifact: scala-identity_2.13-1.0.jar
```

将 JAR 放在定义旁，或设置相对路径。`publish --artifact` 相对 shell 工作目录，`operator.yaml` 中的 `artifact` 相对定义文件。现有示例定义已指向构建输出。

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/scala/operators --root .operator-catalog
```

新版本选择 `publish` 或 `seed` 之一。`publish` 拒绝已有名称/版本，重复 `seed` 验证而不替换。代码变化须发布新版本并更新任务引用。名称和版本匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。Python 定义使用 `executor: python`、`entrypoint: identity` 和 `.py`/`.whl` 制品。完整容器使用 `executor: container`，省略 `artifact`/`entrypoint`，镜像入口实现相同 Arrow 边界。

## 5. 上传到远程仓库主机

向管理员获取文件接收地址和批准的运行时镜像。下列主机名/路径是示例，管理员须创建接收和目录路径并授予权限。这不通过公开网站发布。

```bash
# Contributor: transfer the small JAR to an administrator-provided directory.
scp examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  repository-user@repository-host:/srv/dacp/incoming/

# Administrator: run on repository-host, from its dacp-codex checkout.
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --root /srv/dacp/operator-catalog --image dacp/operator-jvm:1.1 \
  --executor jvm --entrypoint example.ScalaIdentity \
  --artifact /srv/dacp/incoming/scala-identity_2.13-1.0.jar
```

发布时管理员 Docker Engine 必须已有运行时镜像，才能固定其 ID 并导出归档。如已安装，只传小代码文件；否则先传一次环境再运行发布命令：

```bash
# Contributor, only when the repository machine lacks the required runtime.
docker save -o /tmp/dacp-jvm-runtime.tar dacp/operator-jvm:1.1
scp /tmp/dacp-jvm-runtime.tar repository-user@repository-host:/srv/dacp/incoming/
# Administrator, on repository-host:
docker load -i /srv/dacp/incoming/dacp-jvm-runtime.tar
```

Python 传 `identity.py` 或 wheel，使用 `--executor python --entrypoint identity` 和 Python 运行时镜像。发布会复制代码，之后修改接收目录的文件不影响已发布版本。

## 6. 启动、浏览并执行

如果仓库未启动，使用**与发布相同的目录根路径**启动：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve \
  --root .operator-catalog --host 0.0.0.0 --port 8820
```

远程示例应把 `.operator-catalog` 改为 `/srv/dacp/operator-catalog`。已启动服务无需重启即可看到新发布内容。公开部署使用 [HTTPS 参数](../dacp-operator-repository/README.md#https-与运行) 或部署中的 HTTPS 代理。

浏览 `http://localhost:8820/?q=identity`，或检查清单：

```bash
curl -f http://localhost:8820/v1/operators/scala_identity/1.0
curl -f http://localhost:8820/v1/operators/identity_python/1.0
```

页面分别展示代码/运行时大小、摘要、下载地址及入口。手动下载时读取清单的 `artifact.sha256` 与 `runtime.archive.sha256`，请求 `/v1/blobs/sha256/<digest>`，使用前验证 SHA-256。

执行节点配置如下，localhost 替换为节点可达的仓库主机：

```yaml
remote_operator_library_urls:
  - http://localhost:8820
operator_cache: /var/lib/dacp/operators
```

缓存目录须允许节点进程写入。修改配置后重启节点。保存下面任务为 `custom-job.yaml`，把输入 URI 替换为真实且获授权的数据集或 Item：

```yaml
job: custom-operators
tasks:
  - id: scala-copy
    inputs: [dacp://localhost:8815/demo/events]
    operator: scala_identity@1.0
    params: {}
    output: scala_result
  - id: python-copy
    inputs: [scala_result]
    operator: identity_python@1.0
    params: {}
    output: python_result
```

`dacp-shell` 当前用于浏览和查询数据集，没有算子或任务提交命令。可在 Bash 终端使用下面的 `Job.run`。安装 Python SDK 并启动协调节点后，使用授权客户端证书提交。以下路径为本地示例证书：

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
.venv/bin/python - <<'RUN_JOB'
from pathlib import Path
from dacp import Job, Source

outputs = Job.run("dacp://localhost:8815", Path("custom-job.yaml").read_text(), timeout=180)
for name, uri in outputs.items():
    print(name, uri)
    with Source.open(uri) as dataset:
        for item in dataset.list():
            print(item.path, item.format)
RUN_JOB
```

使用 wheel 时将 `identity_python@1.0` 改为 `identity_wheel@1.0`。无需修改客户端或核心注册表。输出按执行节点 TTL 过期。

## 7. 发布前验证

构建 Scala 示例及 Python 环境后，运行真实容器契约测试：

```bash
(cd dacp-server && DACP_TEST_OPERATOR_PACKAGES=1 sbt 'testOnly dacp.OperatorPackageSpec')
```

现有测试覆盖示例 JAR、Python 源码和 wheel 的 Arrow 边界。自己的转换应测试代表性输入、非法参数/格式、多个 batch/输入、预期内容与 schema，以及失败场景。不能只检查退出码，应解码输出并验证转换结果。科学数组/分组应采用显式表示，不应静默展开为记录。

实现细节与执行器插件注册见 [算子打包](operator-packaging.md) 和 [插件契约](plugins.md)。

# 编写与发布算子

DACP 算子消费命名数据项流，并产生命名输出流。可以发布小型 Scala/Java JAR、Python 源码或 wheel，或完整容器；依赖放入共享运行时镜像。仓库算子可在 NORMAL 或 DEBUG 模式执行；[内嵌源码](operator-source.md)仅允许 DEBUG 模式。

仓库网站允许公开搜索、查看和下载。发布可通过管理员 CLI，或默认启用的 [token 认证 HTTP API](../dacp-operator-repository/README.md#token-认证发布-api)。贡献者可由管理员运行 `publish` / `seed`，也可使用发布 token，通过 `scripts/upload-operator.py` 上传本地生成的算子目录。

## 执行契约

启动器传入 `operator /input /input/params.json`。`/input/manifest.json` 的每个输入条目包含 `port`、`file`、`representation`、`format`，以及可选的 base64 Arrow IPC schema 消息。输入文件为包含 Arrow IPC 流的 FIFO。bytes 视图使用名为 `data` 的单个 binary 列；records 保留原始 Arrow schema。遇到 Arrow EOS 即结束，不等待操作系统 EOF。多个输入须按端口名称字典序消费。

stdout 写协议帧，stderr 写诊断。每帧先写两个大端 32 位长度值（JSON 头和载荷），然后写头与载荷字节。每个输出端口依次发送 `start`、零个或多个 `data`、`end`。全部输出的 start 必须先于任何 data。start 声明 `port`、`representation`、`format`；records 还包含 base64 IPC schema。bytes 载荷为原始字节块；records 载荷为一个独立 Arrow IPC 流，内含一个记录批次。每个头上限 64 KiB，载荷上限 8 MiB。非零退出表示失败；已发送结果保留为部分结果，只有成功退出后才报告任务完成。

输出直接流向客户端或下游任务，没有输出目录、保存的数据集或 `_output.json`。容器根目录和输入只读，私有临时文件放在 `/tmp`。容器禁用网络，并限制内存、PID、临时空间与执行期限，支持取消。依赖必须预置在运行时中。新的科学数据表示须先定义共享契约，不得默默展平结构。

## Scala 示例

完整的 [ScalaIdentity.scala](../examples/operators/scala/src/main/scala/example/ScalaIdentity.scala) 使用 [build.sbt](../examples/operators/scala/build.sbt) 独立编译为薄 JAR。它消费 `source` 字节输入，并向 `result` 保留原始字节块和格式。[Uppercase.scala](../examples/operators/inline/Uppercase.scala) 演示相同协议下的数据转换。

在项目根目录运行：

```bash
scripts/build-operator-image.sh
(cd examples/operators/scala && sbt package)
(cd dacp-operator-repository && sbt stage)
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish scala_identity@1.0 \
  --image dacp/operator-jvm:1.1 --executor jvm --entrypoint example.ScalaIdentity \
  --artifact examples/operators/scala/target/scala-2.13/scala-identity_2.13-1.0.jar \
  --ports examples/operators/scala/operators/scala_identity/1.0/operator.yaml \
  --root .operator-catalog
```

构建与发布需要 JDK 17、sbt、Linux 和 Docker 权限。普通 JAR 执行无需在运行时内安装编译器。共享依赖标为 `Provided`，仅打包应用代码与运行时缺少的依赖。多个 main 类可共用同一个 JAR 和运行时。

## Python 示例

完整的 [identity.py](../examples/operators/python/identity_python/1.0/identity.py) 保留字节块或 Arrow 记录批次。共享运行时 [Dockerfile](../examples/operators/python/Dockerfile) 使用 Python 3.12 和 PyArrow 23.0.1。

```bash
docker build -t dacp/operator-python:1.0 examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_python@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact examples/operators/python/identity_python/1.0/identity.py \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

wheel 使用示例 [pyproject.toml](../examples/operators/python/pyproject.toml)：

```bash
python -m pip wheel --no-deps --no-build-isolation --wheel-dir /tmp/dacp-wheels examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 \
  --image dacp/operator-python:1.0 --executor python --entrypoint identity \
  --artifact /tmp/dacp-wheels/dacp_example_ops-1.0-py3-none-any.whl \
  --ports examples/operators/python/identity_python/1.0/operator.yaml \
  --root .operator-catalog
```

预先安装本地构建工具。wheel 使用 `--no-index --no-deps` 离线安装到临时 `/tmp`，pip 输出写入 stderr。运行依赖由镜像提供。不同模块可共用同一个 wheel 和运行时。

## 端口声明与批量发布

必须提供 `publish --ports FILE`；该文件可为包含 `inputs`、`outputs` 的 YAML 或 JSON，也可直接使用完整算子定义：

```yaml
name: identity_python
version: '1.0'
image: dacp/operator-python:1.0
executor: python
entrypoint: identity
artifact: identity.py
inputs:
  source:
    representations: [bytes, records]
    formats: []
outputs:
  result:
    representations: [bytes, records]
    formats: []
```

每个端口声明表示（`bytes`、`records`）、格式限制（`[]` 表示不限），以及可选的 Arrow schema JSON 值。任务必须准确绑定所有声明的输入端口。定义文件位于 `<directory>/<name>/<version>/operator.yaml`，制品路径相对于定义文件；直接 CLI 制品路径相对于当前目录。container 执行器不设置 artifact 与 entrypoint，由镜像入口实现相同协议。

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/python --root .operator-catalog
```

`publish` 拒绝覆盖已有名称与版本；`seed` 验证已有不可变条目。名称与版本须匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。运行时归档和代码按摘要验证并共享。提供已有仓库的下载服务不需要 Docker。

## 传输与执行

通过管理员批准的渠道（例如 `scp`）传输 JAR、源码或 wheel 以及端口定义。若缺少运行时，先以 `docker save` 传输一次，并在仓库主机使用 `docker load` 导入。发布按已安装镜像的不可变 ID 导出。然后启动仓库：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve \
  --root .operator-catalog --host 0.0.0.0 --port 8820
```

DACP 节点配置 `remote_operator_library_urls: [http://repository-host:8820]`，或将定义与代码放入 `local_operator_library`。连接 shell 后运行：

```text
run identity_python@1.0 --input source=/reports/orders
run scala_identity@1.0 --input source=/reports/orders
```

shell 等待并逐块显示结果，不保存输出数据集。另见[流契约](operator-streams.md)与 [DEBUG heredoc 源码](operator-source.md)。

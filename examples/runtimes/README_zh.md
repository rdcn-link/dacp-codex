# 常用算子运行环境

仓库在 `/runtimes` 列出已注册的执行环境；开发者提交源码时通过精确的 `name@version` 选择环境。多个算子共用运行环境归档。每套环境均通过 SHA-256 固定执行镜像和源码构建镜像。这些示例适用于 Linux x86-64。

| 运行环境 | 主要依赖 | 源码语言 | 用途 |
| --- | --- | --- | --- |
| `python-arrow@1.0` | Python 3.12.3、PyArrow 23.0.1 | Python | Arrow 记录流、CSV 和 SQLite 分析 |
| `jvm-arrow@1.0` | OpenJDK 17.0.20.1、Scala 2.13.16、Arrow 18.3.0 | Java、Scala | 共用 Arrow 和 DACP SDK 的 JVM 算子 |
| `python-scientific@1.0` | Python 3.12.3、PyArrow 23.0.1、NumPy 2.2.6、Pandas 2.2.3、SciPy 1.15.3 | Python | 数值分析和科学记录转换 |

JVM 执行镜像为 `dacp/operator-jvm:1.1`；其独立构建镜像为 `dacp/operator-scala-debug:1.0`，使用 [`build-scala-source-runtime.sh`](../../scripts/build-scala-source-runtime.sh) 准备。Java 和 Scala 共用相同的执行依赖，因此使用同一个目录条目。Python 环境的构建和执行使用同一个镜像。

## 构建 Python 科学计算环境

在项目根目录，使用 Linux x86-64 上的 Python 3.12 下载固定版本的二进制 wheel，再禁用网络进行构建。这样不依赖精简执行镜像内部的证书或软件包下载工具。

```bash
python3.12 -m pip download --only-binary=:all: \
  --dest examples/runtimes/python-scientific/target/wheels \
  -r examples/runtimes/python-scientific/requirements.txt

docker image inspect --format '{{.Id}}' dacp/operator-python:1.0
# 将返回的 sha256:... 镜像 ID 填入 PINNED_BASE_IMAGE。
docker build --network none --build-arg BASE_IMAGE=PINNED_BASE_IMAGE \
  -t dacp/operator-python-scientific:1.0 examples/runtimes/python-scientific

docker run --rm --network none --entrypoint /usr/local/bin/python \
  dacp/operator-python-scientific:1.0 \
  -c 'import numpy, pandas, scipy, pyarrow; print(numpy.__version__, pandas.__version__, scipy.__version__, pyarrow.__version__)'
```

多阶段构建避免在最终镜像中保留下载的 wheel 归档。科学计算依赖会增加共用运行环境的体积，但单个算子产物仍可保持较小。Pandas 和许多 SciPy 算法需要将一定规模的数据放入内存：安装这些依赖不意味着算法本身就是流式算法。

在算子仓库管理页面注册执行镜像和构建镜像，提供固定的镜像 ID、`docker save` 归档及依赖版本。Python 选择执行器和构建语言 `python`；JVM 环境选择执行器 `jvm` 和构建语言 `java, scala`。仓库的源码构建仍然保持隔离且不访问网络。参见[源码提交与运行环境管理](../../docs/operator-submission_zh.md)。

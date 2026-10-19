# 原生命令算子示例

[English](README_en.md)

此 C 程序从标准输入读取字节，并将大写转换后的字节写入标准输出，不依赖 DACP 或 Arrow。`command` 执行器在容器中桥接这些流。

在项目根目录运行：

```bash
mkdir -p examples/operators/native/build
cc -O2 -Wall -Wextra -Werror -s \
  -o examples/operators/native/build/uppercase examples/operators/native/uppercase.c
```

按[算子指南](../../../docs/operator-authoring.md)构建或安装共享的 `dacp/operator-python:1.0` 运行环境。该环境提供桥接所需的 Python 和 PyArrow 依赖；可执行文件必须匹配其 Linux 架构和可用 C 库。静态链接可选。

将执行服务器的 `local_operator_library` 设置为 `examples/operators/native/operators` 的绝对路径，或初始化公开仓库：

```bash
python3 scripts/package-operator-sources.py
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/native/operators --root .operator-catalog
```

为执行节点配置 `remote_operator_library_urls`，以便从仓库下载。在已连接的 shell 中运行：

```text
run native_uppercase@1.0 --input source=/reports/orders
```

二进制文件单独下载，并复用 Python 运行时镜像。发布过程无需保留执行权限，适配器会创建可执行的私有副本。输出始终为流，不会注册为结果 Dataset。

如果程序已安装在运行环境中，省略 `artifact`，并将 `entrypoint` 设置为其在运行环境内的绝对路径。`executor_config.args` 提供按字面值传递的参数。参见[原生命令配置与发布](../../../docs/native-commands.md)。

验证使用真实服务器、mTLS 和 Docker，临时端口为 18817：

```bash
(cd dacp-server && sbt -batch stage)
.venv/bin/python scripts/test-native-execution.py
```

运行时需要 Docker 组访问权限，例如必要时将最后一条命令放入 `sg docker -c '...'`。测试会编译程序，并验证超过管道缓冲区大小的输出、部分输出后失败、增量交付、取消和恢复；结束后清理服务器与临时文件。目前尚未实现原生源码提交、记录转换和多输入输出管道。

附带源码的新版本及可选的许可证、SBOM 发布方式参见[源码与审查信息](../../../docs/operator-provenance.md)。初始化仓库前须先打包源码。

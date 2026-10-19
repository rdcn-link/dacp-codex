# 小型 Python 算子

[English](README_en.md)

本示例将 `identity.py` 与可复用的 Python/PyArrow 环境分开分发。源码制品只有几 KB；Python 和 PyArrow 按运行环境摘要只下载一次。算子通过标准 Arrow IPC 边界保留输入字节，并处理 FIFO 的 EOS 结束标记。

在项目根目录运行，需要 Docker 访问权限和已通过 stage 构建的算子仓库：

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
python3 scripts/package-operator-sources.py
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators examples/operators/python --root .operator-catalog
```

在任务中引用 `operator: identity_python@1.0`，并配置执行节点的 `remote_operator_library_urls` 以连接仓库。也可以从 `dacp-server` 启动节点并设置 `local_operator_library: ../examples/operators/python`。此可选示例不包含在默认初始目录中。构建运行环境时需要联网获取 Python 镜像和固定版本的 PyArrow 依赖；任务执行时不使用网络。

附带的 `pyproject.toml` 将相同源码构建为可安装的 `identity` 模块。在构建环境安装 setuptools 和 wheel 后，构建并发布：

```bash
.venv/bin/python -m pip wheel --no-deps --no-build-isolation --wheel-dir examples/operators/python/dist ./examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 --root .operator-catalog --image dacp/operator-python:1.0 --executor python --entrypoint identity --artifact examples/operators/python/dist/dacp_example_ops-1.0-py3-none-any.whl --ports examples/operators/python/identity_python/1.0/operator.yaml
```

依赖必须已安装在运行环境中。执行器使用 `--no-index --no-deps` 离线安装 wheel，然后调用其模块。多个算子可以引用同一 wheel 中的不同模块。参见[打包与执行器契约](../../../docs/operator-packaging.md)。

完整源码示例、wheel 打包、远程文件传输、管理员发布和执行步骤见[编写与上传自定义算子](../../../docs/operator-authoring.md)。

附带源码的新版本及可选的许可证、SBOM 发布方式参见[源码与审查信息](../../../docs/operator-provenance.md)。初始化仓库前须先打包源码。

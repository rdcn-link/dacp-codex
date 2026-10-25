# 两节点气象协作

[English](README_en.md)

节点 A 发布合成气象观测，节点 B 发布站点气候基准。Union 协调每日统计与距平计算的数据流，客户端仅连接 Union，使用其公开数据集命名空间。

## 准备

分别在 `dacp-server` 和 `dacp-union-server` 执行 `sbt test stage`，按 [Python 算子](../operators/python/README.md)说明构建运行环境。在仓库根目录执行：

```bash
bash scripts/certs.sh
.venv/bin/python examples/union/generate_data.py
.venv/bin/python examples/union/prepare.py
```

准备程序创建稳定 Union 身份、绑定 Union 证书的节点服务凭据以及用户 Token。秘密写入受保护文件，不打印。再次运行会创建更多凭据，旧凭据可通过本地工具撤销。

## 启动

在对应工作目录的不同终端运行：

```bash
# dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-a.yaml
# dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-b.yaml
# dacp-union-server
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
# Repository root
PYTHONPATH=dacp-client .venv/bin/python examples/union/client.py
```

客户端在线读取目录，解析公开数据集名称，将 Item URI 作为工作流变量提交。源步骤输入为空，处理步骤输入仅引用上游输出流。所有算子都需实现结构推导。中间流在节点间传递，最终流经 Union 返回；此例没有显式物化策略。

内网只读看板为 `http://localhost:19880/`。通过 Ctrl-C 或 SIGTERM 关闭服务。状态和执行历史保留，运行中的任务不会在重启后自动恢复。另见[安全说明](../../docs/security.md)、[工作流变量](../../docs/jobs.md)、[策略与重放](../../docs/materialization.md)、[Union 配置](../../dacp-union-server/README.md)。

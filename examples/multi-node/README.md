# DACP 多服务器协作

[English](README_en.md)

只有节点 B 持有 CSV 数据源。节点 A 协调包含两个任务的流式链：B 将数据项转换为 Parquet，再由 A 使用 identity 转发其字节。Flight DoExchange 通过 mTLS 传输任务请求和输出帧，不会创建结果 Dataset 或持久化的输出 URI。

在项目根目录准备环境并运行集成测试：

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
scripts/build-operator-image.sh
bash scripts/test-collaboration.sh
# 同时验证远程仓库下载和校验后的缓存：
DACP_TEST_REMOTE_REPOSITORY=1 bash scripts/test-collaboration.sh
```

需要 Docker 访问权限。测试脚本通过 stage 构建服务器，使用临时端口 18815/18816（仓库为 18820），验证 Parquet 内容、重复执行、上游失败恢复和客户端证书强制验证，随后停止服务器。可以覆盖 `DACP_TEST_PORT_A`、`DACP_TEST_PORT_B` 或 `DACP_TEST_REPOSITORY_PORT`。日志保留在输出的临时目录中；`DACP_PYTHON` 可覆盖默认的 `.venv/bin/python`。

手动运行时，在不同终端中从 `dacp-server` 目录分别执行：

```bash
sbt 'run ../examples/multi-node/node-a.yaml'
sbt 'run ../examples/multi-node/node-b.yaml'
```

手动配置使用端口 8815/8816。从项目根目录提交：

```python
import os
from pathlib import Path
from dacp import Job

os.environ['DACP_CA'] = 'examples/certs/ca.crt'
os.environ['DACP_CERT'] = 'examples/certs/client.crt'
os.environ['DACP_KEY'] = 'examples/certs/client.key'
with Job.run('dacp://localhost:8815',
             Path('examples/multi-node/job.yaml').read_text(), timeout=180) as events:
    for event in events:
        if event['type'] == 'data':
            print(event['port'], len(event['data']))
```

重复执行可以安全复用输出端口名称。提前关闭迭代器会取消流水线。所有输出结束后才发送成功的 `complete` 事件；如果交付数据后发生失败，已显示的输出只是不完整结果。慢速消费者会施加背压。流不能重新打开。

任务可以显式指定 `node`，否则根据输入锚点确定位置。`task_peers` 授权任务分发，`task_input_prefixes` 授权工作节点读取输入源，已发布的数据集默认可浏览和读取。系统没有中心注册表。跨机器部署时，应将 localhost 替换为对外公布的地址，提供匹配的证书 SAN，并配置允许的节点及数据源前缀。

每个算子在隔离容器中运行。`identity@1.1` 保留流式字节（包括 Parquet 文件尾），并与 `format_convert@1.1` 共享 JVM 运行环境。每个输出端口最多有一个消费者，多输入必须按字典序消费。尚未实现通用调度、扇出、重试、检查点和重放。参见[流式契约](../../docs/operator-streams.md)。

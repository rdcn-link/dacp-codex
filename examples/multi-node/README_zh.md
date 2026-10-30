# 双节点流处理示例

[English](README.md)

[job.yaml](job.yaml) 读取 `dacp://localhost:8816/csv_data/orders` 的 records 视图，并传给端口 8815 的协调节点上的 `piflow_csv_save@1.0`，结果以 CSV 字节流返回。运行前，请在仓库发布并审批该算子，配置两个节点使用该仓库，并配置协作凭据。地址和 Item 路径需按实际部署调整。

在项目根目录设置协调节点的 `DACP_CA` 和 `DACP_TOKEN_FILE`，然后执行：

```bash
PYTHONPATH=dacp-client .venv/bin/python examples/multi-node/check.py
```

节点配置示例展示数据发布与传输配置；凭据目录和仓库 URL 需根据部署提供。维护中的服务集成测试会自行创建隔离节点与测试算子：

```bash
bash scripts/test-collaboration.sh
```

所有输出流结束后才报告执行成功，关闭客户端结果流会取消任务。部署详情见[安全说明](../../docs/security_zh.md)、[任务执行](../../docs/jobs_zh.md)和[算子发布](https://github.com/rdcn-link/xstream-codex/blob/main/docs/operator-submission_zh.md)。算子均为普通的已审批仓库包，服务器不包含 identity 或格式转换实现。

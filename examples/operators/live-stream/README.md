# 持续输出的数据流示例

[English](README_en.md)

`live_ticks@1.0` 使用 Python SDK，每秒输出一条模拟气象观测 JSONL，序号从 1 开始。数据为合成演示值。算子代码为几 KB，复用现有 Python/PyArrow 运行时。输入 `source` 仅用作触发：先逐块读完，再开始生成，不缓存整个输入。当前 Job 契约要求至少一个输入。

## Shell 观察

在项目根目录运行，使用现有演示节点 A：

```bash
.venv/bin/dacp-shell dacp://localhost:18861 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

```text
dacp-shell> use weather
dacp-shell> run live_ticks@1.0 --input source=/observations --params '{"interval_seconds":1,"max_records":0}' --timeout 300
```

输出类似：

```json
{"sequence":1,"timestamp":"2026-10-23T10:30:00+00:00","station_id":"DEMO-001","temperature_c":20.5}
{"sequence":2,"timestamp":"2026-10-23T10:30:01+00:00","station_id":"DEMO-001","temperature_c":20.99}
```

Shell 默认没有结果预览上限，数据到达后逐块显示；运行过程中按 Ctrl+C 取消，随后可继续输入命令。`max_records: 0` 表示持续生成，仍受服务器和请求的任务时限约束；不是永久任务。`interval_seconds` 范围为 0.01–60 秒，默认 1；`max_records` 为非负整数，默认 0，设为 5 可自动结束。没有后台调度器，背压会阻塞输出写入，不累积待发送记录。

从 Union 使用时，先以 `datasets` / `use` 选择公开 ID，成员还需审批允许 `live_ticks@1.0`；本地节点直接执行不需要 Union 审批。

## 构建、发布和验证

构建需要一个含 `piflow_csv_parser@1.0` 清单和运行时 blob 的本地仓库，用以复用已有 Python 环境。发布通过 token 认证 API，不包含 Web 上传功能：

```bash
python3 examples/operators/live-stream/test.py
python3 examples/operators/live-stream/build.py --runtime-catalog .operator-catalog --output /tmp/live-ticks-catalog
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
python3 scripts/upload-operator.py live_ticks@1.0 --catalog /tmp/live-ticks-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

源码归档同时提供代码、构建脚本、测试和双语说明。没有额外的第三方依赖。验证流式执行应记录第一条输出与任务结束的时间，并在持续生成时取消，再检查节点释放任务槽。单凭动态图形或较小的输出文件不足以证明流式执行；算子可自行采用阻塞算法，平台流接口不保证每种算法都能立即输出。

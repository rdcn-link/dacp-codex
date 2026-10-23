# 持续输出的数据流示例

[English](README_en.md)

`live_ticks@1.0` 使用 Python SDK，每秒输出一条模拟气象观测 JSONL，序号从 1 开始。数据为合成演示值。算子代码为几 KB，复用现有 Python/PyArrow 运行时。这是零输入源算子，声明 `inputs: {}`，启动后直接生成数据，无需读取任何 Item。

## 端口声明

算子清单和作业中的输入都为空；只有 `result` 输出端口：

```yaml
inputs: {}
outputs:
  result:
    representations: [bytes]
    formats: [ndjson]
```

## Shell 观察

在项目根目录运行，使用现有演示节点 A：

```bash
.venv/bin/dacp-shell dacp://localhost:18861 --ca examples/certs/ca.crt --cert examples/certs/client.crt --key examples/certs/client.key
```

```text
dacp-shell> use weather
dacp-shell> run live_ticks@1.0 --params '{"interval_seconds":1,"max_records":0}' --timeout 300
```

输出类似：

```json
{"sequence":1,"timestamp":"2026-10-23T10:30:00+00:00","station_id":"DEMO-001","temperature_c":20.5}
{"sequence":2,"timestamp":"2026-10-23T10:30:01+00:00","station_id":"DEMO-001","temperature_c":20.99}
```

Shell 默认没有结果预览上限，数据到达后逐块显示；运行过程中按 Ctrl+C 取消，随后可继续输入命令。`max_records: 0` 表示持续生成，仍受服务器和请求的任务时限约束；不是永久任务。`interval_seconds` 范围为 0.01–60 秒，默认 1；`max_records` 为非负整数，默认 0，设为 5 可自动结束。没有后台调度器，背压会阻塞输出写入，不累积待发送记录。

从 Union 使用时，先以 `datasets` / `use` 选择公开名称，成员还需审批允许 `live_ticks@1.0`；本地节点直接执行不需要 Union 审批。

## 构建、发布和验证

构建需要一个含 `piflow_csv_parser@1.0` 清单和运行时 blob 的本地仓库，用以复用已有 Python 环境。发布通过 token 认证 API，不包含 Web 上传功能：

```bash
python3 examples/operators/live-stream/test.py
python3 examples/operators/live-stream/build.py --runtime-catalog .operator-catalog --output /tmp/live-ticks-catalog
export DACP_REPOSITORY_PUBLISH_TOKEN="$(cat .operator-catalog/.publish-token)"
python3 scripts/upload-operator.py live_ticks@1.0 --catalog /tmp/live-ticks-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

源码归档同时提供代码、构建脚本、测试和双语说明。没有额外的第三方依赖。验证流式执行应记录第一条输出与任务结束的时间，并在持续生成时取消，再检查节点释放任务槽。单凭动态图形或较小的输出文件不足以证明流式执行；算子可自行采用阻塞算法，平台流接口不保证每种算法都能立即输出。

## 多步骤纯流工作流

[workflow.yaml](workflow.yaml) 依次执行 `live_ticks → live_ticks_decode → piflow_select_field → piflow_convert_schema → piflow_csv_save`，从 JSONL 字节流转为 Arrow 记录流，选列、重命名，再输出 CSV。所有步骤逐条或逐批处理，无需等待无限输入结束。`live_ticks_decode` 使用明确的气象记录 schema，保留每条观测，不是通用 JSON 展平器；每行最大 64 KiB。

[stream-only-policy.json](stream-only-policy.json) 禁止中间结果物化并设置总时限 300 秒。数据负载通过内存有界队列、FIFO 管道和进程标准输出传递，不保存中间结果文件。执行日志、状态记录、运行时缓存和启动参数文件仍会写入服务器文件系统；本例保证的是中间数据负载不落盘。

在项目根目录启动 Shell，连接节点 A 并选择数据集后提交：

```text
use weather
submit examples/operators/live-stream/workflow.yaml --policy examples/operators/live-stream/stream-only-policy.json
```

终端会持续输出 `sequence,station_id,timestamp,air_temperature_c` 的 CSV 行，按 Ctrl+C 停止。生成器不需要输入 Item。工作流不指定 node，直接提交给所连接节点；通过 Union 提交需审批全部五个算子版本，由调度器选择执行节点。

构建和发布解码算子：

```bash
python3 examples/operators/live-stream/build.py --operator live_ticks_decode --runtime-catalog .operator-catalog --output /tmp/live-ticks-decode-catalog
python3 scripts/upload-operator.py live_ticks_decode@1.0 --catalog /tmp/live-ticks-decode-catalog --url http://localhost:8820 --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

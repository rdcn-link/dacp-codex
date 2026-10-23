# DACP Union 服务

[English](README_en.md)

`dacp-union-server` 是 Scala 开发的协调服务，一个进程维护一个 Union。成员节点独立运行；Union 主动邀请并轮询，经节点批准后分发计算。节点不需要注册或发送心跳。

Union 对外提供普通 DACP Flight 服务，包括在线数据集目录、Item 元数据与 records/bytes 流、记录扫描和 Job 执行，也提供 S3 只读访问。公开数据集使用稳定短 ID，隐藏成员地址。专用 Union 管理 API 与 HTTP 运维看板保留内部拓扑。完整命名、代理、放置及访问边界见 [Union 代理服务](../docs/union-proxy.md)。

## 构建与启动

要求 JDK 17 和 sbt；在本模块目录执行：

```bash
sbt test stage
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

配置中的路径相对于进程工作目录：

```yaml
host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
client_identities: ["<客户端叶证书 DER 的 SHA-256 指纹，64 位小写十六进制>"]
administrator_identities: ["<管理员证书指纹>"]
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
web:
  host: 127.0.0.1
  port: 19880
s3:
  enabled: true
  host: 0.0.0.0
  port: 20860
```

Flight 使用 mTLS，证书须匹配主机名；Union 和成员兼任客户端，需要 `serverAuth`、`clientAuth` 用途，私钥使用未加密 PKCS#8 PEM。CA 签名不等于计算授权。客户端和管理员使用各自的身份清单；每个作业只允许提交者读取结果或取消。成员连接固定证书指纹。节点证书轮换需重新邀请和审批；Union 证书轮换使用新状态目录。

## 邀请与批准

以下是本地 Union 管理命令，可与服务共用状态：

```bash
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
target/universal/stage/bin/dacp-union-server remove ../.union-demo/x.yaml NODE_ID
```

节点配置启用 `union` 后，由节点管理员批准：

```yaml
union:
  state_directory: ../.union-demo/a
  execution_timeout_seconds: 120
  retention_seconds: 300
```

在 `dacp-server` 目录运行：

```bash
target/universal/stage/bin/dacp-server union list ../.union-demo/a
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union leave ../.union-demo/a UNION_ID
```

审批策略示例：

```yaml
scheduler_identity: "<Union 证书指纹>"
input_prefixes: [dacp://localhost:18861/climate]
operators: [daily_temperature_stats@1.0]
max_concurrent_tasks: 4
expires_at_ms: 4102444800000
```

审批绑定 Union ID、证书身份、输入前缀、算子版本、额度和有效期。移除先请求成员撤销，再删除本地记录；网络失败会明确报错并保留记录。撤销或移除停止受影响任务。临时离线标为未知，不自动删除成员。目录按需在线读取，短 ID 路由持久化；成功完整列表确认消失后清理旧路由，不保存无限增长的墓碑，递增计数器不回退。

## 客户端与任务

普通客户端可使用 `Source.datasets`、`Source.open`、`Job.run` 和 Shell 连接 Union，使用公开 URI，省略执行位置或使用逻辑节点 ID。多输入在一个成员处理，跨节点输入流式传递。最终流经 Union 返回客户端。详见 [代理服务及任务示例](../docs/union-proxy.md)。

专用异步接口适合独立查询状态、取消及订阅事件：

```python
from pathlib import Path
from dacp import Union

with Union('dacp://localhost:19860', cert='examples/certs/client.crt',
           key='examples/certs/client.key', ca='examples/certs/ca.crt') as union:
    with union.submit(Path('job.yaml').read_text()) as execution:
        print(execution.job_id)
        for event in execution.results():
            if event['type'] == 'data':
                print(event.get('batch', event['data']))
```

`execution.status()`、`cancel()`、`events(after=0)` 分别查询、取消和订阅。Scala 的 `dacp.client.Union` 提供对应方法；关闭 execution 和迭代器以释放资源。结果只能领取一次；断开最终结果连接会取消未完成任务，关闭状态订阅不会取消。普通客户端使用公开命名空间；管理员可通过异步提交明确指定内部成员位置。完整运行实例见 [双节点示例](../examples/union/README.md)。

## 策略、物化和执行语义

独立策略 JSON 控制 `materialization`、`max_retries`（0–10）和 `timeout_seconds`。输出默认只流式传递；显式物化保存到产生输出的节点，通过新的执行重放。自动重试要求客户端感知尝试失败并丢弃前次部分输出，不强制物化最终结果。总超时覆盖排队及全部尝试。配置、调用和插件契约见 [执行策略](../docs/materialization.md)。

引擎先准备所有节点再启动，容量不足时等待。节点间中间流直接传输；每个连接使用有界队列和 Flight 背压。收到 `end` 不代表成功，还要验证 `complete` 和 Flight 正常结束。取消、故障、撤销和超时清理整个作业。Union 可通过普通 worker 契约参与上级协作，不需要专用嵌套协议；路径检测循环并限制深度。

每作业最多 64 个步骤、1024 个连接端口，每步骤最多 32 个输入及 32 个连接输出。Union 最多 64 个活动作业和 4096 个内存会话，成员须同时预留其子任务额度。状态事件最多保留 128 条、4 MiB，旧游标返回 gap。历史元数据持久化，运行中任务不在重启后自动恢复；物化内容可显式重放。当前不支持输出扇出、内联源码和算子内部检查点。备份应包含成员、路由、审批与执行记录。

## 管理 API 与看板

Flight 管理 Action 使用 `dacp.union.` 前缀，JSON 消息上限 1 MiB：

| Action | 请求 |
| --- | --- |
| submit | `{"job_yaml":"...","execution_policy":{}}`，策略可选 |
| replay_job | `{"job_id":"ID","tasks":["ta"]}`，tasks 可选 |
| job_status / cancel_job | `{"job_id":"ID"}` |
| members | `{}`，管理员 |
| invite_member | `{"url":"dacp://node:18861"}`，管理员 |
| remove_member | `{"identity":"指纹"}`，管理员 |

结果 Ticket 为 `{"type":"dacp.union.results","job_id":"ID"}`，事件 Ticket 为 `{"type":"dacp.union.events","job_id":"ID","after":0}`；事件订阅要求管理员身份。普通 DACP 任务使用 `DoExchange`，目录使用 `dacp.catalog`，逻辑节点使用 `dacp.nodes`。

HTTP 看板展示真实节点、任务脚本、状态、流量、DAG 和分页历史，不领取结果。每两秒刷新；DAG 使用本地 AntV G6，支持水平/垂直依赖布局和泳道、缩放、平移与分组折叠。`GET /api/overview`、`/api/jobs/ID`、`/api/history?offset=0&limit=100` 为只读管理接口。省略 web 则不监听；回环监听供本机访问，对外监听必须配置 token_file。令牌及登录配置见 [管理边界](../docs/union-proxy.md#管理边界)。S3 是独立公开只读数据接口。

## 开发与验证

`dacp-union-protocol` 提供传输、mTLS 与 JSON 契约；`UnionWorker` 复用插件数据访问和执行器。`ProxyCatalog` 通过 `DatasetAccess` 转发，不加入格式或后端分支。领域算子通过现有插件及算子仓库机制扩展。

```bash
# 本模块目录
sbt test
# 项目根目录，需安装 Playwright 和 Chromium
python scripts/test-union-dashboard.py
```

网页位于 `src/main/resources/web/`；`dag.js` 维护图及视口，`app.js` 管理只读请求，文本使用 Canvas/DOM 安全渲染。本地 `@antv/g6@5.1.1` 保留 MIT 许可证；升级时同步版本文件名、HTML、静态路由和测试。正常构建不下载前端依赖。协议细节见 [Union 协议](../docs/union.md)，持久化位置见 [执行记录](../docs/execution-history.md)。

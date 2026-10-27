# DACP 节点 Web 看板

[English](node-web_en.md)

每个 dacp-server 默认启动公开、只读的 HTTP 看板。默认监听 `0.0.0.0`，端口为 Flight 端口加 1000，例如 Flight `8815` 对应 HTTP `9815`，Flight `18861` 对应 HTTP `19861`。启动日志打印实际监听地址。Flight 端口大于 64535 时需要显式设置 HTTP 端口。

```yaml
web:
  enabled: true
  host: 0.0.0.0
  public_host: node-a.example.org
  port: 19861
```

关闭看板：

```yaml
web:
  enabled: false
```

`enabled` 必须为布尔值，`port` 必须为 0–65535 的整数；0 由操作系统分配临时端口，嵌入式程序可通过 `server.webPort` 获取。HTTP 端口绑定失败会使节点启动失败并释放已占用的 Flight 端口和资源。关闭节点同时关闭 HTTP 服务。

首页展示节点 Flight 地址、启动时间、运行时长、任务容量、可用执行器、数据集目录和分页执行记录。选择记录查看 G6 DAG：横向布局、服务器分组、步骤标题、状态、已观测流量，支持点击详情、缩放与平移。节点与 Union 复用同一份绘图代码、样式和离线 G6 资源；都使用项目 DACP Logo。

Union 子任务展示本节点收到的任务定义和步骤，外部输入标为跨节点输入；本地步骤之间保留依赖连线。全局工作流及跨节点调度状态仍以 Union 看板为准。普通 Job 保存脚本，页面根据其定义展示依赖，结合执行记录读取步骤状态。没有记录到的流量显示 `—`。运行中的 Union 子任务按需读取当前内存状态和计数，结束后读取持久化记录；历史记录不包含任务定义时不补造 DAG。

状态与所选任务每 2 秒刷新，数据集目录在打开页面或手动刷新时读取。看板不会打开 Item、运行算子、取消任务或消费结果流。

该页面公开执行元信息及保存的脚本和任务参数，适用于向查看者公开这些信息的部署；可配置监听地址或关闭。Flight 使用 TLS 与 Token；已发布的数据集默认公开，任务执行仍受其独立授权规则约束。Web 返回 `Cache-Control: no-store`，服务静态资源不依赖外网。

Union 提供公开 DACP 代理目录，管理网站也提供按成员查看数据集的页面；见[数据集目录](dataset-directory.md)。

`web.public_host` 是浏览器访问 Web 看板时使用的主机名，默认沿用节点 `public_host`；它与监听地址 `web.host` 分开。节点只在 HTTP 服务成功启动后，通过 Flight `node_status` 报告 `web_enabled: true` 和实际端口对应的 `web_url`。Union 看板成员列表显示该地址链接；关闭 Web 时显示“未启用”，无法取得节点状态时显示未知。

看板界面使用英文。任务标题、数据集名称、JSON 描述和用户脚本保留原文；日期和数值采用英文区域格式。

## 执行来源与内部路由

作业列表仅展示顶层作业，子步骤不再单独占行。作业详情中的 Operator step records 可展开查看各步骤的算子、状态、开始时间、耗时及尝试编号；各次尝试的步骤记录均保留。

作业列表的 Source 列区分 `Direct client`（用户直接提交）与 `Union`（Union 下发）。Type 列显示 `User job` 或 `Union sub-job`。Source 筛选在分页前应用，切换筛选后从第一页开始。详情显示 Union ID、执行 ID 和父记录关联。

看板 HTTP 的 `/api/history?source=client` 与 `?source=union` 按来源查询；省略 `source` 或使用 `source=all` 返回全部顶层作业；过滤步骤后再分页。`/api/executions/<record_id>` 的 `step_records` 包含关联的步骤记录。历史条目与详情包含 `execution_source: client|union`，原有 `kind` 保留精确的记录类型。来源是服务端根据执行记录确定的，不是用户指定的授权字段。

Execution topology 图形化展示本地连接和跨节点边界。Sub-job definition 完整展示持久化记录中实际下发到本节点的步骤定义，保留算子、参数、输入、物化设置和远端 `output_routes`，不筛选或改写字段。本地输入保留 `task:port`，远端输入保留定位对象，不再单独展示路由区域。普通作业展示 Job script。`consumer_identity` 身份摘要用于授权远端读取者，不表示数据类型或传递方式。

DAG 默认锁定鼠标滚轮缩放；点击 🔒 Mouse zoom 解锁后才响应滚轮，再次点击可锁定。工具栏的加减、适应视图和 100% 按钮始终可用。

锁定时，在 DAG 上滚动鼠标滚轮会正常上下滚动页面；仅解锁时由图形接管滚轮。

跨节点输入接口显示在上游服务器的独立区域中，位于本节点执行区域之外。接口卡片标注上游步骤及输出端口（如 `parse.result`）；它不是本节点执行的步骤。依赖图和泳道布局采用同一归属关系。

作业名称为必填的 `name`；执行 ID 由系统生成。节点与 Union 的作业列表统一采用 Name、Job ID、Source、Type、Status、Started at、Duration 列，详情展示名称和完整 ID。作业定义、子作业定义和执行策略统一显示 YAML。

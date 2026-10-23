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

该页面公开执行元信息及保存的脚本和任务参数，适用于向查看者公开这些信息的部署；可配置监听地址或关闭。Flight 保留 mTLS；已发布的数据集默认公开，任务执行仍受其独立授权规则约束。Web 返回 `Cache-Control: no-store`，服务静态资源不依赖外网。

Union 提供公开 DACP 代理目录，管理网站也提供按成员查看数据集的页面；见[数据集目录](dataset-directory.md)。

`web.public_host` 是浏览器访问 Web 看板时使用的主机名，默认沿用节点 `public_host`；它与监听地址 `web.host` 分开。节点只在 HTTP 服务成功启动后，通过 Flight `node_status` 报告 `web_enabled: true` 和实际端口对应的 `web_url`。Union 看板成员列表显示该地址链接；关闭 Web 时显示“未启用”，无法取得节点状态时显示未知。

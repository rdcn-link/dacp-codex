# 任务执行记录

DACP Server 和 Union Server 默认持久化执行元数据，不能通过配置关闭。执行记录不保存结果流负载；输出物化由独立的执行策略及物化存储控制。节点保存普通作业的 Job 脚本，以及 Union 子作业的完整步骤定义（含参数、输入定位和输出路由）；Union 保存全局 Job 脚本。脚本可能包含参数或提交的源码，看板会公开展示这些记录。

## 用户与运维

DACP Server 的记录目录默认为进程工作目录下 `.dacp-executions/PORT`。可在服务器 YAML 中指定：

```yaml
execution_store:
  provider: file
  settings:
    directory: /var/lib/dacp/node-a/executions
```

U 使用 `state_directory/executions`。不同运行实例应使用独立目录；同一目录只允许一个执行记录写入者。历史元数据不受 `retention_seconds` 限制，不自动删除；该选项仅清理内存中的执行会话、结果队列和状态事件窗口。管理员可备份记录目录。

记录包含独立 `record_id`、类型、提交者、节点、任务或执行关联 ID、算子、输入引用、状态变化、起止和更新时间、耗时、可获得的端口字节与记录计数、失败或取消原因。内嵌程序只记录运行时标识；错误信息最多 2048 个字符，可能包含程序报告的诊断。DACP 普通 Job 与实际执行的 worker task 分别记录，用 `parent_execution_id` 关联；Union 的全局 Job、节点执行与单个 task 分别记录，用共享 `execution_id` 和 `parent_record_id` 关联。

在对应模块目录，可使用本地 CLI 查询，服务运行期间也可以读取：

```bash
# DACP 节点
 target/universal/stage/bin/dacp-server history list /var/lib/dacp/node-a/executions
 target/universal/stage/bin/dacp-server history list /var/lib/dacp/node-a/executions 100 100
 target/universal/stage/bin/dacp-server history get /var/lib/dacp/node-a/executions RECORD_ID

# U
 target/universal/stage/bin/dacp-union-server history list ../.union-demo/x/executions
 target/universal/stage/bin/dacp-union-server history get ../.union-demo/x/executions JOB_ID
```

Union HTTP 看板的“执行历史”可分页查看 U 的持久化作业、任务状态与流量。`GET /api/history?offset=0&limit=100` 查询历史；`GET /api/jobs/ID` 在内存会话失效后仍返回历史元数据。列表最大分页大小为 1000，网页每页 100 条，搜索和状态筛选作用于当前页。历史记录与看板公开只读，无需登录或 token。历史数据不能用于重放结果、恢复 Job 或重新授权读取源数据。

服务器重启发现未结束的记录时将其标为 `interrupted`（服务中断），并记录 `interruption_detected_at_ms`。未知的真实结束时刻不会伪装成确定时间：`finished_at_ms` 与 `elapsed_ms` 取最后观测时刻，具体状态变更检测时间见事件。正常关闭时已清理的任务记录为取消或已结束。保留历史不会自动恢复任务。

## 开发

共享实现为 `dacp-server/ExecutionHistory.scala`，U 复用相同实现。每次执行开始前写入记录，在状态变化、Union 进度观测与结束时更新。单个 JSON 文档同时包含最新快照和有序状态事件，避免快照与事件分开提交。写入临时文件并同步到磁盘，然后原子替换记录，同步父目录；写入失败会使对应请求失败，不静默禁用记录。目录写锁防止两个实例同时回收对方未完成的执行，CLI 使用只读模式。

`TaskService` 覆盖普通 Flight Job 与直接 worker 请求；`UnionWorker` 记录节点执行和每项 task；`Coordinator` 记录全局作业。记录标识独立于用户提供的任务名称，因此重复运行同一任务不会覆盖旧记录。终态不会被迟到的运行状态覆盖。当前采用目录扫描、按创建时间排序的分页，适合初期部署；尚无数据库索引、自动归档或远端汇总节点历史的功能。U 持久化的是其观察到的节点状态；每个节点的本地记录保留本地任务状态转换。

验证：`cd dacp-server && sbt test`，`cd dacp-union-server && sbt test`。构建两个模块的 `sbt stage` 并准备示例证书和 `dacp/operator-python:1.0` 镜像后，在根目录运行 `python3 scripts/test-execution-history.py` 和 `python3 scripts/test-union.py`，验证真实 Docker 执行及历史记录。两个脚本使用临时端口和目录，并自动关闭启动的服务。

Union 看板展示提交的 Job YAML 和数据集、任务、端口依赖及最终输出组成的 DAG 图。看板和只读查询接口公开展示这些信息，历史文件由服务器管理员管理。节点健康信息包含 JVM 进程启动时间 `started_at_ms` 和单调计时的已运行时间 `uptime_ms`；节点不可达时保留上次观测值。未保存脚本的旧记录明确提示脚本不可用。

## 存储扩展

节点和 Union 都支持上述 `execution_store` 映射。`provider` 选择通过 ServiceLoader 注册的 `ExecutionStoreProvider`，实现自行校验私有 `settings`。默认 file 位于 `plugins/dacp-plugin-execution-file`，`ExecutionHistory` 保留状态转换、中断恢复和分页。应用可用 `new ExecutionHistory(store, ownsStore = false)` 保留存储所有权。见[插件注册](plugins_zh.md#实例环境和执行存储)。

## 看板中的作业与步骤

节点看板 Jobs 列表仅展示顶层作业，通过 Source 筛选 `Direct client` 或 `Union`。步骤记录保留在持久化存储中，在所属作业的 Operator step records 展开查看状态、开始时间、耗时及尝试编号。来源筛选与步骤排除均在分页前完成。

Execution topology 展示本地连接和跨节点边界；Sub-job definition 完整展示收到的步骤定义，不另设路由文件或路由展示区域。远端输入使用 `producer_identity`，远端输出使用 `consumer_identity`；本地连接无需证书身份。

# 数据流与控制依赖作业

[English](jobs_en.md)

DACP Server 与 Union 使用相同的 Job 格式。`tasks` 描述步骤，`inputs` 只连接其他步骤的输出流，`depends_on` 描述执行前置条件，Job 的 `outputs` 选择返回客户端的结果流。列表顺序不代表执行顺序。

```yaml
job: weather-ingestion
tasks:
  - id: prepare
    title: 初始化目标表
    operator: prepare_weather_table@1.0
    inputs: {}
  - id: read
    title: 读取观测记录流
    operator: read_item@1.0
    inputs: {}
    params:
      uri: dacp://localhost:18861/weather/observations
      view: records
  - id: write
    title: 增量写入观测
    operator: write_weather_table@1.0
    inputs:
      source: read
    depends_on: [prepare]
outputs:
  summary: write:summary
```

`prepare_weather_table` 和 `write_weather_table` 是业务算子名称示意，使用前需要自行实现并发布；前者可声明 `outputs: {}`，后者的 `summary` 返回完成后的写入摘要。通过 Union 的普通客户端提交时，读取 URI 必须使用 Union 公开的数据集名称，省略 `node` 或使用 `dacp.nodes` 返回的逻辑节点名称。管理员可指定成员的实际地址。

## 流连接

输入值必须是字符串 `task` 或 `task:port`；省略端口表示 `result`。Job 最终输出采用相同写法。不存在默认端口时明确报错，不选择其他端口。不再支持 `{uri, view}`、`{task, port}` 对象或连接处的 `view`。

源算子声明 `inputs: {}`，可以读取外部数据或自行生成数据；普通步骤只消费其他步骤的输出。输出元数据包含实际表示和 schema，接收端按算子声明验证；不做隐式 bytes/records 转换。每个输出最多有一个消费者，不能同时作为下游输入和客户端结果；复制应使用显式复制算子或多个输出端口。

## 内置读取源

`read_item@1.0` 是服务端内置的传输算子，无需下载容器。`params` 必须包含且只包含字符串 `uri`、`view`；URI 定位单个 Item，`view` 为 `records` 或 `bytes`。输入为空，输出为 `result`。读取通过公共 Dataset/Item 插件接口完成，保留结构、背压、取消及资源释放语义；不物化完整输入。读取目标参与权限校验与数据就近放置，不能通过参数绕过成员授权或任务输入前缀。

```text
run identity@1.1 --input source=/observations --view records
```

Shell 自动生成 `read_item` 步骤，并把 `run` 的输入连接到读取步骤。`--view` 只选择 Item 读取表示；生成的连接不携带 `view`。零输入生成器仍可直接 `run live_ticks@1.0`。

Shell 默认接收 `result`；多输出可重复指定 `--output PORT`，无输出步骤使用 `--no-output`。

## 控制依赖与零输出

数据连接允许上下游同时运行。`depends_on: [prepare]` 则等待前置步骤成功退出后才启动，不以最后一帧或输出 EOS 作为成功证明。本机使用执行状态；跨节点通过带证书身份校验、限定接收者的 Flight 状态请求确认，不需要创建假的数据流。两个节点之间仍直接传输数据，Union 不转发中间数据。

两类依赖共同参与未知引用、重复引用和环检测。控制前置不能同时位于该步骤的流式上游路径，否则可能因背压死锁，应拆分任务并显式使用物化/重放边界。DAG 中实线表示流连接，紫色虚线表示控制依赖。

算子可声明 `outputs: {}`，Python/Scala SDK 允许成功结束时不输出帧；原生命令可将 `executor_config.stdin` 或 `stdout` 设为 `null`，同时将对应端口声明设为 `{}`。无输出步骤、独立分支和控制前置都需要执行，不能因未选入最终结果而丢弃。Job 可以设置 `outputs: {}`，客户端等待整体成功或失败，不接收数据结果。

## 故障、重试与资源

前置失败会阻止下游启动，并取消本次执行的关联任务和流；取消、超时及成员撤销沿用执行策略。重试以新的执行尝试运行，控制依赖也重新检查。物化只保存数据输出，不保存外部系统的副作用，也不代表数据库事务；重试可能重复执行建表、写入或通知，业务算子必须自行保证幂等性。重放已物化的步骤不会重新执行其已被替代的前置工作。

控制依赖和传输源是执行边界，不把它们当作普通容器算子合并。内存队列、Flight 和 FIFO 维持背压；日志、代码缓存和显式物化属于独立的存储用途。详细策略见[物化和重放](materialization.md)。

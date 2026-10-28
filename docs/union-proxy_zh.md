# Union 的 DACP 代理服务

[English](union-proxy.md)

Union 同时提供标准 DACP 服务、Union 专用 API 和独立 HTTP 运维看板。Python/Scala `Source`、`Job` 和 Shell 可以连接 Union 的公开 DACP 地址。成员节点仍独立运行；Union 连接配置中的成员并轮询，使用节点签发的服务凭据代理数据和分发计算。

## 公开数据集命名

节点继续以本地名称发布数据集。Union 将“成员证书身份、本地数据集名称”映射为固定长度的公开名称，例如 `ds-k7m2-9wqr`，公开 URI 为 `dacp://union.example:19860/ds-k7m2-9wqr`。Item 在其后追加逻辑路径。`name` 就是该公开名称；整个系统的数据集没有独立的 `id` 或显示名称属性。成员本地名称仅用于内部路由。成员地址、证书身份不写入公开目录；用户提供的描述和数据内容原样保留，发布者应自行控制其中的信息。

名称使用持久化递增计数器、密钥化可逆置换和不易混淆字符编码生成，不能从字符串直接看出相邻整数关系。编码不是访问授权手段。逻辑节点 ID 使用 `nd-xxxx-xxxx`，供需要指定计算位置的客户端使用；通过 Flight Action `dacp.nodes` 获取。

`dacp.nodes` 的每个节点包含逻辑 `id`、服务地址 `address`、状态 `state`、容量 `capacity` 和观测时间。可用时还提供任务占用、Union 槽位、启动时间、运行时长、资源使用情况、执行器和看板地址。不可用成员仍展示并明确标注状态；保留指标属于最后一次观测。设计器显示地址和状态详情，禁止把步骤放置到不可用成员，执行位置仍保存逻辑 ID。

映射保存在 `state_directory/dataset-routes/routes.json`，包含编码密钥、下一计数值和当前路由。不保存数据集元数据副本。该目录只允许一个服务实例持有写锁。备份时保留整个 Union 状态目录；不要用旧备份覆盖仍被外部引用的 名称空间。密钥丢失或计数器回退可能破坏 名称稳定性。

## 在线目录与成员变化

`dacp.catalog` Action 接收空请求体，返回 `Directory` protobuf：`datasets`、`complete`、`unavailable_sources`。每次请求在线读取已批准成员的目录并合并，新发现的数据集自动获得名称。成员不可达、未批准或目录读取失败时返回可用部分，并标记不完整；客户端不能把部分目录当作完整清单。标准 `ListFlights` 无法携带完整性标记，因此遇到部分目录返回 `UNAVAILABLE`。

临时故障保留原路由；只有一次成功完整的成员目录读取确认名称消失，才删除对应映射。明确移除成员也会清理其映射。删除的公开名称不再使用；同名数据集再次出现会获得新公开名称。撤销批准立即停止代理，故障或撤销不会把旧公开名称指向其他数据集。节点原地替换同名数据集不能被名称契约识别为新数据集；这是当前身份模型的边界。

## 客户端使用

```text
dacp-shell dacp://union.example:19860
dacp-shell> datasets
dacp-shell> use ds-k7m2-9wqr
dacp-shell> ls /
dacp-shell> cat /observations records
```

Shell 的启动参数绑定服务器地址。`datasets` 不接收地址参数。完整目录只有一个数据集时自动选择；直接使用公开名称选择，例如 `use ds-k7m2-9wqr`，或执行 `datasets` 后通过 `use 1` 选择对应行。列表只显示行号、名称和 JSON 描述，不显示 URI。SDK 的 `Source.datasets(...)` 返回可迭代目录并提供完整性字段；访问 Item、记录扫描和 bytes 流沿用普通 DACP 接口。S3 默认开启、匿名只读，以公开名称 作为 bucket，Item 逻辑路径作为 key；使用 `s3: {enabled: false}` 可关闭。S3 不能表达部分 bucket 清单，目录不完整时返回 503。

## 普通任务与位置选择

通过标准 `Job.run` 或 Shell `submit` 提交普通 Job；输入只引用 Union 的公开 URI：

```yaml
name: climate-summary
tasks:
- id: read_summarize_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://union.example:19860/ds-k7m2-9wqr/observations
    view: bytes
- id: summarize
  title: Parse observation CSV
  operator: piflow_csv_parser@1.0
  inputs:
    source: read_summarize_source
outputs:
  result: summarize
```

例中的算子须已安装或可从仓库获取。普通任务可省略 `node`。Union 在已认证的配置成员中按容量筛选执行位置，优先数据或上游步骤所在成员；全部原始输入大小已知时选择需要传输总字节数较少的位置，否则按输入声明顺序选择。一个多输入算子只在一个成员执行，其他成员输入通过数据流传输。需要固定位置时设置 `node: nd-xxxx-xxxx`，不能在公开任务中填写内部地址。

执行策略 YAML、结果流、失败尝试通知、取消、超时、成员上的物化和重放沿用现有执行引擎。结果默认流式返回，经 Union 到客户端。Union 作为上级的成员时复用普通 worker 契约，转交本地成员执行；输入使用短期、单消费者、绑定证书身份的流，不落盘。代理路径限制为 8 层并检测循环；取消和剩余执行时间传到下级。

## 管理边界

成员由 Union 启动配置指定，使用节点签发的服务凭据。普通客户端使用 TLS 与 Token；任务接口及结果、事件流检查所有者。不提供远程成员管理 API。内网 HTTP 看板公开只读，会展示运维拓扑。S3 使用独立 SigV4 凭据。见[安全说明](security_zh.md)及 [Union 配置](../dacp-union-server/README_zh.md)。

## 开发与验证

`DatasetCatalog.directory` 扩展目录完整性契约，默认包装插件的 `list`。`ProxyCatalog` 通过相同的 `DatasetAccess` 契约转发元数据、扫描和流，不识别具体数据库或文件格式。插件仍使用现有注册方式，无须修改核心注册表。记录型 Item 的共享 JSONL bytes 视图同样适用于代理与 S3。

```bash
(cd dacp-union-server && sbt test)
(cd dacp-server && sbt 'testOnly dacp.DatasetCatalogSpec dacp.DacpServerSpec dacp.S3ServiceSpec')
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```

契约测试覆盖名称编解码与重启、完整/部分目录、公开 URI、Item 流、S3、自动放置、跨成员原始输入、组合 worker、权限与循环检测。

Union 作为上级成员需要物化输出时，可在 Union 配置顶层设置 `materialization_store`，使用相同存储插件契约；存储在该 Union 层，不要求其下游成员物化：

```yaml
materialization_store:
  provider: file
  settings:
    directory: .union-state/materializations
```

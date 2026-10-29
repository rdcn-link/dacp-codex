# XStream 执行引擎

XStream 使用有界、支持背压的数据流执行 YAML Job。`xstream-engine` 是可嵌入的 Scala 模块，内含 `xstream-runner` 启动程序；启动程序不单独建模块，不启动 DACP 服务、看板或监听端口。DACP Server 内嵌同一个引擎，Union 继续负责成员分配和可执行子作业下发。独立宿主会自动将封闭的远程数据源处理区域下推到已配置的 DACP 服务。

## DataCatalog 生命周期

统一层次为 **DataCatalog → Dataset → Item**。一个 JVM 通过 `DataCatalogRuntime` 绑定一个 DataCatalog。宿主可为同一实例申请租约；已有目录时不能绑定另一个目录，最后一个租约负责关闭目录。应用必须先停止 Job 再释放目录。启动不要求预加载全部 Dataset、Item 或数据内容，插件可以动态发现它们。

DACP Server 根据 `datasets` 或 `data_catalog` 初始化目录，内嵌的 `JobEngine` 使用同一实例。`context:` 读取通过直接、有界的队列传递，不绕回网络；远程 `dacp:` 读取仍通过配置的数据服务。数据源和格式保持插件化。

## Item 引用

`read_item@1.0` 没有输入端口，输出端口为 `result`。寻址参数统一为 `uri`：

```yaml
params:
  uri: context:/weather/observations
  view: records
  sql: SELECT station_id, temperature FROM source LIMIT 100
```

`context:/dataset/item` 引用接收方 JVM 的目录，不是操作系统文件路径。`dacp://host:port/dataset/item` 显式指定服务，服务也可位于本机。两种引用都支持相同的 `view` 和受限只读 SQL。旧 `path` 参数已删除。远程下发前，context 引用必须固定到提交目录的数据源，不能在下游成员的目录中重新解释。

Folder Item 通过 `DatasetAccess.list(path)` 或相应 SDK 逐层浏览。`read_item` 不隐式拼接或递归读取文件夹。

## 不启动服务器直接执行

```bash
cd xstream-engine
sbt stage
cd ..
xstream-engine/target/universal/stage/bin/xstream-runner \
  examples/xstream/engine.yaml examples/xstream/read.yaml
```

启动程序接受 `CONFIG.yaml JOB.yaml [VARIABLES.yaml] [POLICY.yaml]`。没有变量但需策略时，传入空变量 YAML 映射。记录以 JSONL 输出，字节流直接写到 stdout。为避免混淆，启动程序要求选中一个输出；嵌入式引擎支持多个具名输出。执行元信息记录到 `execution_directory`；只有策略明确要求物化时才持久化流数据。

远程读取在 `nodes` 中配置连接：

```yaml
nodes:
  - url: dacp://node-a.example.org:18861
    ca: /etc/dacp/ca.crt
    token_file: /etc/dacp/node-a.token
```

凭据留在宿主配置中，Job 只包含 Item URI。启用自动下推后，远程读取及其后续处理可作为子作业提交到该服务。

## Scala 嵌入

```scala
import dacp._
import dacp.union.YamlDocument
import java.nio.file.{Files, Path}

val settings = YamlDocument.parse(Files.readString(Path.of("engine.yaml")))
val plugins = new PluginEnvironment()
val host = new LocalEngine(settings, plugins)
val cancel = new StreamCancel()
try {
  val engine = host.engine
  assert(engine.dataCatalog eq DataCatalogRuntime.catalog)
  engine.run(Files.readString(Path.of("job.yaml")), frame => {
    // 增量消费元信息和数据，不保留无限增长的结果。
    println(StreamProtocol.header(frame))
  }, cancel)
} finally {
  cancel.close()
  host.close()
  plugins.close()
}
```

`LocalEngine` 可接收远程 Item 回调。`EngineEnvironment` 提供运行配置、历史记录、物化和 Item 打开能力；`JobEngine` 不依赖服务器配置、服务端凭据或请求接入类。取消、超时、重试、物化/重放和兼容算子同进程合并属于共享引擎。结构推导仍是显式的设计时能力，不作为执行前置条件。

## 目录与运行插件

实现 `DataCatalogProvider`，在 `META-INF/services/dacp.DataCatalogProvider` 中注册完整类名，配置为：

```yaml
data_catalog:
  provider: example-jdbc-catalog
  source:
    jdbc_url: jdbc:h2:/srv/catalog
```

提供者返回线程安全的 `DataCatalog`。其 `open(name)` 返回调用者持有的 `PublishedDataset` 租约，再打开 `DatasetAccess`，使用后关闭两个句柄。`DatasetProvider`、`FormatHandler` 和算子执行器继续使用现有契约，详见[插件](plugins_zh.md)。启动程序打包标准数据源、基础格式、静态目录、执行存储和算子执行器；更多插件通过 `plugin_directory` 加载。

算子必须来自经过审核公开发布的仓库，使用名称/版本标识。本地执行使用仓库产物和校验过的缓存，不允许通过启动程序加载任意源码或 JAR。`read_item` 是内置数据源，无需算子容器。

## 设计器

`xstream-designer` 模块提供浏览器编辑器。`local_catalog` 使用相同的 `datasets`/`data_catalog` 配置；`nodes` 配置额外 DACP 连接，执行目标 `target` 也列入目录。侧栏初始只获取目录入口，展开入口加载 Dataset，展开 Dataset 或 Folder Item 加载直接子项。失败仅影响当前分支，折叠后重新展开可重试。

本地选择生成 `context:/dataset/item`，节点选择保留完整 DACP URI。本地结构和样例预览使用设计器目录。在别处执行导出的 Job 时，需要提供匹配的上下文目录，或显式更改 Item URI。

## 自动计算下推

独立的 `LocalEngine` 默认启用自动下推。YAML Job 保持逻辑定义，不指定执行节点。服务地址及普通客户端凭据配置在宿主：

```yaml
compute_pushdown_enabled: true
nodes:
  - url: dacp://node-a.example.org:18861
    ca: /etc/dacp/ca.crt
    token_file: /etc/dacp/node-a.token
    compute_pushdown: true
```

设置 `compute_pushdown_enabled: false` 可保留本地计算。节点的 `compute_pushdown: false` 表示仅访问数据；省略时允许尝试提交计算，目标服务器仍检查授权、资源、算子仓库及运行环境。配置本身不授予权限。引擎不要求 Union 组网或专用凭据，规划也不调用结构推导。

对于 `read(A) → filter → aggregate`，将封闭处理链作为普通 Job 提交到 A，远程读取改成 A 目录中的 `context:` 引用。对于 `read(A) → filterA` 和 `read(B) → filterB` 两条分支再合并，各分支下推到其数据源，合并留在本地。返回输出通过有界队列和背压接入本地，不需要中间数据文件。保留多个边界端口以及用户定义的 Job 输出名称。同一服务上的多个不相连封闭区域可以共用一个子作业。

规划基于数据就近原则，不是通用成本优化器。只下推全部输入依赖均在同一已配置远程服务、且至少包含一个处理步骤的区域。单独的远程读取仍直接获取数据。本地 `context:` 读取、无远程 Item 依赖的源算子、本地与远程数据汇合及其后续处理保留在本地。不向任意远程 worker 上传本地输入，也不安排 A 到 B 的直传；需要集中协调跨节点分配时仍使用 Union。独立远程分支会并发消费。

执行策略只要选择了任何物化输出，或者是显式重放，就保留原本地图和持久化归属。普通流式重试和超时策略仍作用于物理执行。远程子作业自身不独立重试、不物化输出；重试由发起引擎协调，消费者须按流协议丢弃失败尝试的部分输出。远程请求受发起引擎客户端剩余时限约束；接收服务保留自身可能更短的执行时限。取消会关闭远程结果流，并取消服务器端执行。远程失败直接传播，不自动回退本地以免重复副作用。

执行历史保存原 Job、`placements`、带下发 YAML 的 `remote_subjobs`，以及包含 `remote_execution_id` 和最终状态的 `remote_job` 子记录。包下载、运行环境缓存和执行元数据可能使用磁盘；流边不物化数据。嵌入宿主可以通过 `EngineEnvironment.computePushdown` 提供自己的 `ComputePushdown` 适配器；`LocalEngine` 也接受可选的 `remoteCompute` 适配器。DACP Server 的现有嵌入宿主不会隐式开启这项独立宿主策略，也不绕过现有调度。

另见[下推 Job 示例](../examples/xstream/pushdown.yaml)。修改远程 URI 和宿主配置后，使用 `xstream-runner` 执行。

接收服务为 Union 时，`context:/公开数据集名/Item` 通过其公开数据集到成员的映射解析。显式使用该 Union 配置公布的服务地址具有相同含义。若使用可识别的成员节点 URI，读取步骤则定位到对应的数据归属成员。

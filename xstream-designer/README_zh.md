# XStream 设计器

[English](README.md)

`xstream-designer` 是独立的 Scala HTTP 模块，提供英文的浏览器图形化 Job 设计界面，无登录、无用户管理。复用普通 Scala SDK 访问节点或 Union，设计器不执行作业，不依赖服务器内部实现。

## 启动

需要 JDK 17、sbt 1.10.x。先复制并修改 [example.yaml](example.yaml)，将地址、CA 与 Token 文件设置为部署环境的实际值。凭据只保存在服务端配置中，不返回浏览器。

```bash
cd xstream-designer
sbt test stage
target/universal/stage/bin/xstream-designer /etc/dacp/designer.yaml
```

默认地址为 `http://127.0.0.1:8840/`。需要网络访问时设置 `host: 0.0.0.0`。服务配置由管理员维护，访问网页无需登录；配置的上游普通客户端凭据决定网页可读取的目录和可请求的推导能力。

## 设计流程

1. 打开页面后自动加载本地目录及已配置的服务根；`target.url` 可选。设计器始终把目标视为普通 DACP 服务；Union 自行决定如何拆分和分配步骤。
2. 自动从配置文件的 `repository` 读取 `/v1/operators`。可搜索算子名称和说明、选择版本、拖到画布。`read_item@1.0` 是内置数据源。
3. 打开左侧 **Catalogs** 标签页，展开目录时加载数据集，展开数据集和文件夹时加载 Item。点击数据项为选中或新建的读取步骤设置 URI 和视图。Union 连接使用 Union 的公开 URI。
4. 拖动卡片调整位置，点击输出端口启动随鼠标移动的预览线，再点击输入端口建立流依赖；右键或 Esc 取消。端口不支持拖拽连线。在右侧修改标题和参数；支持删除步骤、移除连接、撤销、重做与自动排列。
5. 画布始终有且仅有一个 **Outlet**，可拖动位置，但不能删除或复制。点击算子输出端口，再点击 Outlet，即创建 Job 输出绑定；Outlet 最多连接一个步骤的一个输出端口，新连接替换原绑定。属性侧栏仅编辑步骤属性与参数；结果流名称可在 Job YAML 中修改。选中连到 Outlet 的线并按 Delete 可删除对应绑定，Outlet 本身始终保留。导入文件中的 Job 输出也连到同一个 Outlet。布局保存在浏览器草案中，导出时默认写入 `designer` 辅助区。允许 `outputs: {}`；每个生产者端口最多一个消费者。每个输出端口下方展示表示、格式和可确定的 Schema 字段；未知结构明确标注。
6. 在 **Variables** 中编辑或加载独立 YAML 变量文件。参数可使用 `$INPUT_URI` 等完整变量值，保留整数、布尔和复合结构的类型。变量值不嵌入模板导出文件，使用 **Export variables** 单独导出。
7. 参数、变量及连接变化后自动推导输出结构，参数编辑停顿 650 毫秒后触发，合法修改自动保存并同步到导出的作业；未完成或非法输入保留在表单中并提示。设计器使用公共 `dacp-operator-inference` 模块，在自身环境中按需启动并复用推导 worker，从配置的算子仓库获取代码；普通算子的推导不经 Union 或成员节点。`read_item` 从本地目录或已配置服务查询 Item 元信息，上游变化也会更新下游。推导请求串行合并，过期响应丢弃。在右侧 **Outputs** 标签页查看当前步骤各输出端口的完整结构及有界样例。推导不运行 Job；结构返回后再请求有界样例，`read_item` 可读取受限的源数据行。打开 Outputs 页直接展示已有结果，不重复推导。首次推导可能启动 worker。参数不完整或推导失败时明确提示，不保留过期字段，也不猜测未知结构。
8. 点击 **Export job YAML** 保存可供 Shell / SDK 提交的标准 Job，导出会打开 YAML 预览窗口，默认勾选 **Include designer metadata**，可取消以只导出执行定义；提供 **Copy**、**Print** 和 **Download**。后续可以通过 **Load YAML** 加载以前导出的 Job，或者在 Job YAML 面板编辑后点击 **Apply YAML**。接受当前公开 Job 格式；内部子作业路由和旧接口格式不作为导入格式。

浏览器使用本机 localStorage 自动保存草案；不同浏览器不共享草案，没有服务器端历史作业存储。加载“历史文件”指上传已有 YAML 文件，不是从运行看板拉取执行历史。设计器不在导出时执行推导；导出检查图结构，实际输入端口及 schema 校验由设计时推导提供。布局支持平移滚动、缩放和适应视图。

提交示例：

```text
submit workflow.yaml --vars variables.yaml
```

下侧 **Logs** 面板记录当前浏览器会话的操作提示、API 请求起止与耗时、请求错误、结构推导诊断以及浏览器异常，按 Info / Warning / Error 分类，可过滤、清空。日志按时间从新到旧排列，新消息显示在顶部；开启自动跟随后保持在顶部，关闭后新增消息尽量保持原阅读位置。上方状态栏仅显示最新一条消息，完整消息保留在日志面板。日志只在浏览器内存中保留最近 1,000 条，不写入磁盘；页面刷新后清空。节点和服务进程的完整后台日志仍保存在各自日志文件中。

## 聊天编排

聊天返回草案及说明，**Apply proposed changes** 后才替换当前图，支持撤销。模型输出与普通草案使用相同的图结构校验，并限制为所连接仓库的算子。它不会执行作业。

默认 `chat.provider: builtin` 是离线命令助手，**不是大语言模型**。支持英文／中文命令以及 CSV 过滤模板，例如：

```text
create CSV filter job
add piflow_filter@1.0
connect read to parse:source
set variable LIMIT = 123
rename Weather analysis
```

CSV 模板需要仓库包含相应 parser、filter、CSV 输出算子。模板生成后，用户须从实际目录设置 `INPUT_URI`，并检查过滤条件和执行位置。

自由自然语言编排可直接使用兼容 Chat Completions 的模型服务，例如自部署模型：

```yaml
chat:
  provider: openai-compatible
  url: http://localhost:8000/v1/chat/completions
  model: your-deployed-model
  token_env: DACP_DESIGNER_CHAT_TOKEN
```

`url` 填完整的聊天请求端点。设计器将当前草案、仓库清单、最近 12 条对话发送给模型，并解析返回的 JSON 草案；截断或不合法的响应不会应用。此接口的模型服务约定参见 [vLLM 官方说明](https://docs.vllm.ai/en/latest/serving/online_serving/openai_compatible_server/)。本地示例默认未配置模型，仍可使用离线命令助手。

也可以连接自定义模型适配 HTTP 服务：

```yaml
chat:
  provider: http
  url: https://model-adapter.example/dacp/plan
  token_env: DACP_DESIGNER_CHAT_TOKEN
```

适配服务不必采用特定模型供应商。设计器 POST JSON，字段包括 `instructions`、`message`、`messages`（聊天历史）、`job_yaml`、`variables_yaml`、`operators`（仓库清单）、`origin`。返回单个 JSON 对象：

```json
{"reply":"说明变更","job_yaml":"name: ...\ntasks: ...\noutputs: ...\n","variables_yaml":"LIMIT: 123\n"}
```

`token_env` 可省略；设置后从服务端环境读取 Bearer Token，不在网页输入密钥。HTTP 适配器超时 90 秒，响应上限 1 MiB。需要由部署者提供并配置实际模型适配服务；不绑定模型密钥，也不把离线命令助手描述为通用 AI。

第三方还可实现 `xstream.designer.ChatPlanner`，通过 `META-INF/services/xstream.designer.ChatPlanner` 注册实现类，将 JAR 放入 `plugins/`，使用 `java --add-opens=java.base/java.nio=ALL-UNNAMED -cp "lib/*:plugins/*" xstream.designer.Main CONFIG.yaml` 启动，然后设置 `chat.provider` 为实现的 `name`。无需修改核心注册表。适配器仅负责草案生成，不能绕过公共图校验。外部 JAR 应以 Provided 方式引用匹配版本的设计器契约。

## 接口与开发

HTTP API 不要求网页用户认证，仅供本设计器使用。除配置与静态资源外，使用 JSON POST：

| 路径 | 功能 |
|---|---|
| `GET /api/config` | 公开服务、仓库名称与聊天能力，不返回凭据文件 |
| `/api/operators` | 使用 `repository` 查询仓库实时算子列表 |
| `/api/datasets`、`/api/items` | 读取所连接服务的目录与数据项元数据 |
| `/api/document`、`/api/mapping` | 解析／规范化 Job、参数或变量 YAML |
| `/api/export` | 检查图结构，返回 Job 与变量 YAML |
| `/api/infer` | 使用 `job_yaml`、`variables_yaml` 请求真实结构推导 |
| `/api/chat` | 基于当前草案、消息和实时目录生成候选草案 |

POST 请求上限 1 MiB，仓库列表上限 4 MiB；不接受浏览器传入任意代理地址。CA 与 token_file 是服务端文件。页面只读取目录和结构，不提供作业执行、仓库发布或用户管理 API。

后端用 Scala 实现，浏览器交互使用随包发布的原生 JavaScript / SVG / CSS，不依赖外部 CDN。测试覆盖 YAML 往返、变量类型、循环与重复消费、空输出、真实 Arrow schema 展示、仓库查询、聊天草案及凭据不外泄；需配合浏览器测试验证拖放与连接、导入导出和实际 Union 推导。

浏览器回归测试（需要 Playwright 和 Chromium）：

```bash
python3 scripts/test-job-designer.py --url http://localhost:8840 \
  --item-uri dacp://localhost:19860/DATASET/observations
```

从仓库根目录执行，将 URI 换成实际 records Item，要求包含 `quality` 列。测试只推导结构，不执行作业。

属性面板采用固定右侧栏，默认隐藏，双击步骤或选择 Outlet 时展开，占用独立列而不遮挡画布；关闭后空间还给画布。左右侧栏均通过边缘窄条箭头折叠与展开，不提供关闭按钮。右侧展开时显示 `>`，折叠后显示 `<`；左侧方向相反。点击画布空白处、连线或按 Esc 不会自动隐藏属性侧栏。顶部直接展示具有清晰边框和立体层次的 **Step** 与 **Parameters** 标签页。底部编辑区默认收起，点击标签展开，使用箭头按钮收起；画布优先占用窗口空间，页面本身不滚动。

属性面板分为 **Step**、**Parameters** 和 **Outputs** 三个标签页。Parameters 根据算子必需的静态 `parameters` 契约生成：名称和类型来自声明，同时显示说明、默认值、枚举及范围。可选参数需显式启用，未启用的参数不写入 Job。对象、数组及完整值变量（`$VAR_NAME`）通过表单配置。属性和参数的合法修改自动保存并同步 YAML，保留类型；连续输入合并为可撤销的编辑操作。详见[参数契约](../docs/operator-parameters_zh.md)。

本地目录、服务连接和算子仓库仅通过启动配置中的 `local_catalog`、`target`、`nodes` 和 `repository` 指定，浏览器不提供手动连接、切换或重新加载连接的入口。打开页面时独立加载两个服务，精简页头显示状态及故障信息；上游恢复后刷新页面即可重试。

点击流连线后高亮选中，可在画布获得焦点时按 Delete / Backspace 删除；选中时仅高亮连线，不出现矩形框或额外的工具栏按钮，支持撤销和重做。在表单输入框中编辑文字不会触发连线删除。一个已连接到下游步骤的端口不能同时作为 Job 输出，需要先删除该连线。导入文件中的输出绑定也会显示为连到 Outlet 的线。

选中步骤后按 Delete，会删除该步骤及其相关连线和 Outlet 连线；选中连线后按 Delete，只删除该连线。撤销可恢复完整删除前状态，重做可再次删除。编辑参数或名称等表单字段时不会触发图形删除。

左侧算子库采用可折叠分类与图标网格，直接读取管理员审批指定的多个 `categories` 及开发者提供的 `icon`；分类和图标不再按输入端口推测，无需修改设计器注册表。搜索会自动展开匹配分类，清空搜索后恢复原展开状态。悬停或键盘聚焦图标可查看版本、说明、输入端口、参数类型和执行器；拖入画布、双击或按 Enter 添加步骤。

拖动已有步骤仅调整位置，不会展开已折叠的属性侧栏；单击步骤只选中，双击才展开其属性。

在画布空白处按住左键拖动可框选完全位于选框内的步骤和 Outlet；Shift 框选或 Shift 单击步骤追加或切换选择。多选后拖动任一所选元素整体移动，Delete 一次删除所选步骤及关联连线，可撤销恢复；Outlet 不会被删除。框选与拖动均不会展开属性侧栏，缩放和滚动后同样有效。



选中步骤或连线后，Delete 和 Backspace 均可删除；选中多个步骤时批量删除。编辑输入框、文本区或参数值时不会触发画布删除。画布获得键盘焦点不显示整块蓝色外框。

删除多个步骤前显示确认框；取消时不改变选择、作业和撤销历史，删除单个步骤或连线无需确认。画布上方工具栏中的 Undo / Redo 按钮及快捷键支持撤销和重做：Ctrl/⌘+Z、Ctrl/⌘+Shift+Z，也支持 Ctrl+Y 重做。批量删除和有效移动分别作为一次操作整体恢复。文本输入框内保留原生文本撤销行为；拖动过程中不切换历史。

拖动步骤或 Outlet、以及从算子库新增步骤时，与其重叠的其他步骤及 Outlet 会自动向下避让，并在需要时连锁调整，保留间距。多选步骤保持相对位置，停留在拖动目标位置。每次预览均基于拖动开始前的布局重新计算，移开鼠标后邻近元素恢复；取消拖动会恢复全部位置且不修改历史。放下后整次移动及避让作为一次操作支持撤销和重做。连线不计入放置障碍。

步骤框的右上角显示算子声明的图标及颜色，与左侧算子库一致；内置 `read_item` 使用相同的数据源图标。尚未加载到算子元信息时显示灰色占位图标。

Parameters 使用紧凑表单，参数名为固定标签。已声明单一类型的参数显示类型标注和小型 `$` 变量切换按钮，不再重复显示类型下拉框。说明、默认值与约束通过信息提示查看；可选参数用复选框启用。仍支持多行字符串、嵌套对象、数组、枚举及联合类型。自定义参数仅在新增时输入名称，已有参数名均显示为标签。

左侧栏将 **Operators**、**Catalogs** 分成两个标签页，标签以带图标的圆角按钮沿侧栏左缘从上到下排列，内容显示在其右侧，与右侧横向属性标签区分。默认打开算子页；切换时保留搜索、分类展开、数据集展开及各页滚动位置。支持上下方向键、Home 和 End 切换标签。

打开页面时先加载目录根；展开时才加载数据集和文件夹。切换标签复用已加载的列表及展开项目；刷新页面可重新加载目录根。加载期间合并重复请求，空目录或服务不可用时显示明确状态。

### 导出与恢复布局

`designer.version: 1` 是布局格式版本；`generator.name` 和 `generator.version` 标记设计器名称及构建版本，`exported_at` 是本次导出时的 UTC 时间（ISO 8601）。`positions` 保存步骤坐标，`outlet` 保存结果出口位置，`viewport` 保存缩放和滚动位置。重新加载会恢复这些布局信息；没有辅助区的文件自动排列。执行端接受并忽略整个 `designer` 区，不影响执行语义。预览窗口切换复选框后，复制、打印、下载均使用当前预览内容，变量仍单独导出。

设计器本地推导需要 Docker 访问权限；`operator_cache`、`operator_max_bytes`、可选的 `plugin_directory` 和 `inference_workers` 配置本地运行环境。worker 空闲后自动回收。仅重新推导变化的步骤及其下游；当前浏览器编辑会话内复用未变化的结果，刷新页面会开始新的会话；失败时显示诊断信息，不转发到执行节点兜底。

### YAML 编辑器

Job YAML、Variables 和只读导出预览统一使用 CodeMirror 6。支持 YAML 语法着色、变量着色、行号、代码折叠和搜索；可编辑区域还支持搜索替换、两空格缩进、Tab 缩进、编辑器内撤销/重做和语法错误标记。编辑器内快捷键不会操作画布历史。语法提示仅检查 YAML 语法，作业结构与参数规则仍由 Apply YAML 和服务端检查；不会因此自动执行任务或推导。

画布变更会同步到 Job YAML；手动修改 YAML 后，在 Apply YAML 之前不会被画布同步覆盖。导出预览只读，辅助信息开关、复制、打印和下载使用同一份原始 YAML 内容，不包含行号或着色标记。

参数名称后的问号不再弹出悬浮说明。鼠标移入、键盘聚焦或点击问号时，参数面板下方的说明区显示该项的描述、类型、是否必需、默认值、枚举与约束；内容保留到查看下一项或切换步骤。嵌套参数共用这个说明区。

前端源码在 `frontend/`，依赖版本由 `package-lock.json` 锁定。修改编辑器源码或依赖时，使用 Node.js 22 或以上版本运行：

```bash
cd xstream-designer/frontend
npm ci --ignore-scripts
npm run build
```

将生成的 `src/main/resources/web/yaml-editor.js` 和 `editor-licenses.txt` 与源码一起提交。之后在模块目录执行 `sbt test stage`；常规 Scala 构建和部署无需 Node.js。资源由 Scala HTTP 服务本地提供，浏览器不访问 CDN。第三方许可保留在 `/editor-licenses.txt`。

### 输出结构与样例

输出预览统一位于右侧 **Outputs** 标签页，不再提供顶部手动推导按钮或下侧独立结构预览页。参数和连线变化自动更新各端口的结构。未接齐输入的步骤单独显示缺失端口，上游及无关分支的成功结构不受影响。

样例由算子的 `previewOutputs`（JVM）或 `preview_outputs`（Python）接口提供，不由界面依据 Schema 合成。所有算子必须实现样例接口；当前输入无法生成样例时明确报告原因。每个端口默认最多 10 行、16 KiB（按序列化后的 UTF-8 JSON 计），单次图预览的样例总量最多 256 KiB；截断会明确标记。结构推导与样例请求分离：结构逐步骤返回后，后续样例请求附带 `preview: {max_rows: 10, max_bytes: 16384}`。样例方法不调用完整作业的处理入口。`read_item` 的样例来自受限源读取，records 保留字段结构，bytes 用 Base64 展示。详见[结构推导与样例接口](../docs/structure-inference_zh.md)。

`read_item` 未显式选择 `view` 时，根据 Item 元信息优先选择 records，不支持时使用 bytes；显式选择始终保留。设计器展示自动选择的实际视图，YAML 中省略自动选择的 `view`，执行端也采用相同规则。例如 `dacp://localhost:19860/ds-6zmr-crvh/benchmark` 的 records 视图可返回站点、时间、温度、湿度及质量字段的 Schema。

步骤框内仅以紧凑的字段名和类型展示输出结构，不展示数据样例；完整结构及算子提供的样例统一在右侧 **Outputs** 标签页查看。

结构推导按步骤跟踪。算子、参数、输入绑定和被引用的变量发生变化时，使该步骤及其下游结果失效；独立分支和上游结果继续展示并复用。删除下游步骤不会重新推导保留的步骤。移动、选中、重命名、调整步骤顺序、切换侧栏或修改 Job 输出绑定不会触发推导。结构与样例结果分别跟踪。服务端比较变量解析后的参数和依赖指纹，因此修改无关变量不会调用未变化的算子。

Schema 的界面类型名称参见[结构推导文档](../docs/structure-inference_zh.md#界面中的-schema-类型名称)，步骤框与 Outputs 统一展示，保留嵌套层级、时间单位与可空性。

步骤框的结构采用紧凑的两列表格，分别展示字段名和统一类型名，无额外表头。字段较多时在表格区域滚动，长字段名和类型省略显示，悬停查看完整内容；右侧 Outputs 提供完整结构。

推导错误在步骤框中仅显示错误图标和 `Inference error`，未知结构显示问号图标和 `Schema unknown`；推导中显示 `Inferring…`。详细错误在右侧 Outputs 和下侧 Logs 中查看。

Outlet 最多连接一个步骤的一个输出端口；连接新端口会替换原绑定，允许暂时不连接。导入和导出均校验此限制。结构推导先返回 Schema，再单独请求有界数据样例；样例读取较慢或失败不影响已展示的结构。

## 校验、逐步骤推导和编辑操作

点击 **Validate**，下侧 **Validation** 页会列出错误和警告，点击问题中的步骤名称可定位并打开该步骤。检查包括：未连接 Outlet、没有流向 Outlet 的游离步骤、缺失或多余输入端口、不存在的步骤引用、重复消费、环路、参数及变量。已有推导结果还用于检查输出端口和输入表示／格式是否匹配；没有推导结果时显示未验证警告，不表示验证通过。校验不执行作业、不启动推导 worker、不读取数据，不能保证实际执行成功；当前未应用的 Job YAML 不参与画布校验。

设计器通过 `/api/infer_stream` 逐步骤接收 NDJSON 事件：`step` 携带步骤名与该步骤推导结果，`complete` 携带完整报告，`error` 表示请求未正常完成。推导依赖从上游到下游，完成一个步骤立即更新画布；当前仍串行执行独立分支。发生请求错误时保留本次已返回步骤的结构，过期草案结果被丢弃。有界样例随后单独获取，records 样例在 Outputs 中按字段列和记录行展示为表格；嵌套值用紧凑 JSON 展示，长值悬停查看，bytes 样例显示 Base64。

选中单个步骤后，点击 **Duplicate step** 或按 **Ctrl/⌘+D** 创建副本。副本使用新 ID，保留算子、参数，不复制输入连线和 Outlet 绑定，避免重复消费。可撤销复制。框选或 Shift 单击多个步骤后，拖动其中任一步骤的标题栏可整体移动，保持相对位置，连线跟随，支持整组撤销；选中的 Outlet 也可一起移动。

编辑会话的结果复用仅保存在内存中，不同浏览器会话相互隔离，30 分钟没有推导请求后过期。最多保留 32 个会话，每个会话保存最新步骤结果，合计不超过 2 MiB。这不会为节点或 Union 的推导接口增加持久化结果缓存。如需在不修改参数的情况下刷新源数据元信息，可重新加载页面。

左侧仅有 **Operators** 和 **Catalogs** 两个目录。画布不展示成员节点或节点区域；Job 步骤不包含 `node`，布局不包含 `node_areas`。本地读取使用 context URI；远程读取使用服务公开 URI，目标为 Union 时使用其公开名称。

数据浏览器以树形展示 In-context catalog 与配置的 DACP 服务。初始只列出目录入口，展开时才读取 Dataset 和 Folder Item 的直接子项。本地 Item 设置 `read_item.params.uri: context:/dataset/item`；节点 Item 保留完整的 `dacp://host:port/dataset/item` URI。context 引用属于最终执行的 JVM，因此其目录需要与本地预览目录匹配。节点凭据配置在 `nodes` 中，仅保留在设计器服务器。可选 SQL 使用 `FROM source`，结构和样例预览反映查询结果。

```yaml
local_catalog:
  datasets:
    weather:
      provider: directory
      source:
        root: /srv/weather
nodes:
  - url: dacp://node-a.example.org:18861
    ca: /etc/dacp/ca.crt
    token_file: /etc/dacp/node-a.token
```

设计器通过与 XStream、DACP Server 相同的插件契约绑定 JVM 级 DataCatalog。可用 `local_catalog.data_catalog` 配置自定义 DataCatalogProvider，替代静态数据集。详见 [XStream 引擎](../docs/xstream_zh.md)和 [Job 读取参数](../docs/jobs_zh.md#读取路径远程服务与-sql)。

目录浏览以当前实现为准：左侧 **Catalogs** 初始只加载本地目录及配置的服务根；展开后才加载数据集和文件夹。`target.url` 可选，本地 Item 使用 `context:`，远程 Item 使用完整 DACP URI。`read_item` 的结构及样例读取使用对应目录。

Catalogs 目录树分别用主机、数据集、文件夹和数据文件图标区分层级。启动时加载目录根，展开时加载子项；不提供目录刷新按钮。

Item 行在图标旁使用小型符号表示视图能力：表格符号表示 records，二进制符号表示 bytes；两种视图都支持时同时显示。路径不再追加视图名称，悬停提示或键盘焦点的无障碍标签提供完整说明。

[科学视图选择、结构预览及样例](../docs/designer-scientific_zh.md)：支持数组、图像、序列的插件式表单及紧凑展示。

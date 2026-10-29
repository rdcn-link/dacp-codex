# 源码提交、构建与审核

[English](operator-submission.md)

仓库**仅接受源码提交**。开发者不能提交最终 JAR、wheel、可执行产物或分类。仓库分析源码，在隔离环境构建精简算子包，等待管理员审批。只有通过审批的算子才进入公开搜索、清单和下载。

## 开发者流程

1. 从仓库管理员获取个人提交 Token。
2. 浏览 `/runtimes` 或 GET `/v1/runtimes`，选择兼容且预装所需依赖的固定执行/构建环境。
3. 使用[算子 SDK](operator-authoring_zh.md)实现处理、输出结构推导和有大小限制的输出样例。
4. 将 Python 模块或 Java/Scala 包放在 `src/`，JVM 资源可放在 `resources/`。生成 ZIP/TAR，不包含编译产物、嵌套压缩包、符号链接或必须执行的构建脚本。
5. 编写提交 YAML，通过辅助工具提交。保存返回的提交 ID；提交不等于发布。

示例目录：

```text
weather-filter/
  submission.yaml
  src/
    weather_filter.py
  LICENSE.txt
```

```yaml
name: weather_filter
version: '1.0'
description: Filter observation records by quality
language: python
entrypoint: weather_filter
runtime: python-arrow@1.0
source_public: false
icon:
  path: M3 5h18v14H3zM3 10h18M9 5v14
  color: '#14866b'
inputs:
  source:
    representations: [records]
    formats: []
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
provenance:
  license: Apache-2.0
  license_file: LICENSE.txt
```

运行环境名称/版本必须来自实际仓库。Java/Scala 相应设置 `language`，`entrypoint` 为完整主类名，如 `example.WeatherFilter`，源码放在 `src/example/`。辅助工具从环境目录推导执行器和不可变环境描述。

```bash
# 在 weather-filter/ 中打包原始源码，而非构建产物。
zip -r sources.zip src
# 在 DACP 项目中执行；预先安装 Python SDK 依赖及 PyYAML。
python scripts/submit-operator.py /path/weather-filter/submission.yaml \
  --source /path/weather-filter/sources.zip \
  --url https://operators.example.org \
  --token-file /secure/submitter.token
```

也可用 `--token-env ENV_NAME` 代替 `--token-file`。远程提交要求 HTTPS，仅 localhost 开发允许 HTTP。工具上传源码和可选许可证/SBOM，再 POST `/v1/submissions`；不会上传运行环境或算子产物。HTTP 202 返回 `submission_id` 和 `status: pending`。提交者携带 Token GET `/v1/submissions` 或 `/v1/submissions/{id}`，只能查看自己的记录。Token 撤销后无法提交或查询。

## 管理员流程

访问 `/admin` 登录，在 Submitter Tokens 标签页按提交者名称签发或撤销 Token。新 Token 只显示一次，服务仅保存摘要。同一提交者名称的新 Token 仍代表同一个提交者，不同人员应采用不同名称。

在 Runtimes 标签页注册固定运行环境及兼容构建环境，提供 Docker 镜像归档、镜像 ID、语言/执行器列表、版本和依赖信息。环境目录和环境归档公开。版本不可覆盖。管理员应保证构建环境使用的 SDK、依赖版本与执行环境一致。

Review submissions 标签页展示原始清单、源码公开声明、私有源码下载、扫描报告、构建状态和构建产物；刷新可取得异步进度。审批时可选择已有分类或增加多个新分类。最终可信度为 0–5 分，默认使用扫描得分；调整分数或批准低于 3 分的算子必须说明理由。批准后公开，拒绝后仍私有。已审核的提交不能再次审核；被拒绝的版本可以修正后重新提交，获得新提交 ID。已批准的名称归原提交者，已批准版本不能覆盖。

公众页面和清单显示**管理员评定的可信度**，不代表安全保证。原始扫描分数与详细发现保存在私有审核记录中。5 分表示在分析覆盖范围内未发现问题，不能证明完全可信。静态分析可能漏报恶意行为，也可能误报正常代码。参见 [OWASP 对静态分析局限性的说明](https://owasp.github.io/DevSecOpsGuideline/2-Process/2-3-Build/2-3-1-Static-Analysis/2-3-1-1-Static-Application-Security-Testing/)。

## 源码公开与扫描

`source_public` 必须显式声明。管理员始终可以通过认证后的审核接口下载源码。设为 false 时，公开清单隐藏源码归档、源码仓库/版本及构建说明，公开 blob URL 不能下载这些私有文件。详细扫描报告可能包含源码片段，不对公众开放。设为 true 时，源码随批准的算子一起公开。许可证及 SBOM 在批准后仍作为公开审核材料提供。

内置分析器对 Python、Java、Scala、shell 源码和可读项目元数据执行有大小限制的词法检查，标记网络 API/地址、进程执行、破坏性文件访问、动态/本地代码加载、环境/凭据读取和文件写入。每项发现包含规则、严重程度、文件、行号与有限片段。它不执行跨过程数据流分析、依赖漏洞扫描或行为运行。存在不支持文件或覆盖不完整时自动得分最高为 2；没有识别出可执行源码时为 0。扫描失败不能批准。报告记录覆盖范围与分析器 ID。这是已实现的基础分析器，不是完整 SAST 产品。可通过 `dacp.operator.SourceAnalyzer` 与 Java ServiceLoader 增加分析器，参见[插件说明](../plugins/dacp-plugin-source-analysis/README_zh.md)。

## 隔离构建

Java/Scala 编译为 JAR，Python 完成语法检查后编译为字节码 ZIP。固定构建模板通过 `SourceBuildRecipe` 扩展，不接受开发者指定任意构建命令。管理员的构建镜像必须包含编译器和 `/opt/lib/*` 下的 SDK/依赖。JVM 镜像需要 `/opt/java/bin/java`，Java 需要 `jdk.compiler`，Scala 需要编译器库。Python 镜像需要 `/usr/local/bin/python` 和 DACP SDK 等执行依赖。构建期间不下载依赖。

每次编译使用新 Docker 容器：禁止网络、只读根文件系统与源码挂载、非 root 用户、移除 capabilities、禁止新增权限、最多 64 个进程、1 CPU、512 MiB 内存。`/tmp` 与 `/output` 各为 64 MiB 临时文件系统。不挂载宿主输出目录、Docker socket 或仓库凭据。产物通过限制大小的 stdout 管道返回，诊断日志也有上限。构建超时为 120 秒，产物上限 64 MiB。同时运行一个构建，最多排队 32 个。重启后恢复排队/运行中的待审构建。环境镜像准备阶段另有最多 120 秒时限。构建失败的算子保持私有且不能批准，可由管理员拒绝后重新提交修正版。

归档限制：压缩后 64 MiB、解压后 16 MiB、1000 个条目、单文件 1 MiB。严格检查路径后才解包，拒绝编译产物、嵌套归档、链接和 TAR 特殊文件。Docker 隔离降低风险，但不构成安全证明；宿主内核与管理员提供的镜像仍属于信任基础。

## HTTP 接口

| 接口 | 访问条件 | 用途 |
|---|---|---|
| GET `/v1/operators`、`/v1/operators/{name}/{version}` | 公开 | 仅已批准清单 |
| GET/HEAD `/v1/blobs/sha256/{hash}[/{filename}]` | 公开 | 已批准公开清单或已注册环境引用的文件 |
| GET `/v1/build-languages` | 公开 | 已安装构建插件的语言/执行器/产物元数据 |
| GET `/v1/runtimes`、`/v1/categories` | 公开 | 环境目录与管理员分类 |
| PUT `/v1/blobs/sha256/{hash}` | 提交 Token | 私有源码/许可证/SBOM，最大 64 MiB |
| POST `/v1/submissions` | 提交 Token | 仅源码清单，最大 64 KiB |
| GET `/v1/submissions[/{id}]` | 提交 Token | 本人提交状态 |
| POST `/api/admin/login` | 密码 | 创建八小时管理会话 |
| GET `/api/admin/session` | 管理会话 | 会话与 CSRF Token |
| POST `/api/admin/logout` | 管理会话＋CSRF | 退出 |
| GET/POST `/api/admin/tokens` | 管理会话；POST 要求 CSRF | 查询/签发提交 Token |
| DELETE `/api/admin/tokens/{id}` | 管理会话＋CSRF | 撤销 Token |
| GET/POST `/api/admin/runtimes` | 管理会话；POST 要求 CSRF | 查询/注册环境 |
| POST `/api/admin/blobs` | 管理会话＋CSRF | 通过 `X-File-Name` 流式上传归档，返回文件描述 |
| PUT `/api/admin/blobs/sha256/{hash}` | 管理会话＋CSRF | 上传环境归档 |
| GET `/api/admin/categories` | 管理会话 | 现有分类 |
| GET `/api/admin/submissions[/{id}]` | 管理会话 | 私有审核记录 |
| GET `/api/admin/submissions/{id}/files/{hash}/{filename}` | 管理会话 | 私有材料 |
| POST `/api/admin/submissions/{id}/review` | 管理会话＋CSRF | 结论、分类、可选可信度与说明 |

审核示例：

```json
{"decision":"approved","categories":["Climate","Conversion"],"trust_score":4,"note":"Reviewed network use; destination is controlled."}
```

省略 `trust_score` 时使用已完成的扫描得分。登录 JSON 包含 `password`；后续写请求携带会话 Cookie 和 `X-CSRF-Token`。提交 Token 不能调用管理接口。没有匿名发布或自动批准路径，手工放入本地打包目录的文件也不会自动公开。

Python 字节码和 JVM JAR 可能被反编译。`source_public` 控制原始源码材料的公开，不保证可下载代码的算法保密。Python 构建与执行版本必须匹配。

管理员可以在已发布算子的审核详情中修改分类标签。此操作仅调整公开分类，不改变已批准的构建产物或可信度评分。认证接口为 `POST /api/admin/submissions/{id}/categories`，JSON 请求体示例为 `{"categories":["Data processing","Merging"]}`。

Python/Arrow、JVM 和 Python 科学计算环境的准备方式见[常用运行环境示例](../examples/runtimes/README_zh.md)。

算子详情页提供 **Operator package**（算子程序包）及允许公开的源码、许可证和 SBOM 下载。共用的执行与构建环境统一在 **Runtimes** 目录下载，算子详情页提供对应环境的跳转链接。Docker 镜像 ID 标识执行镜像，归档 SHA-256 用于校验下载文件。

管理员可通过 `POST /api/admin/submissions/{id}/icon` 修改已发布算子的图标，请求示例为 `{"icon":{"path":"M3 4h18v16H3z","color":"#2378cb"}}`。此接口同样要求管理员会话及 CSRF 校验。仅修改校验通过的 SVG 路径和颜色，保留已批准的构建产物、运行环境和可信度评估。

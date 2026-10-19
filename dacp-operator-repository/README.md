# DACP 算子仓库

[English](README_en.md)

独立的 Scala HTTP/HTTPS 仓库，提供公开搜索、版本页面和下载。每个算子由不可变的 `name@version` 清单、共享运行时镜像，以及可选的小型 JAR、Python 源码或 wheel 制品组成。运行时与代码按 SHA-256 只存储一份，可供多个算子引用。提供下载无需 Docker；发布运行时镜像需要 Docker CLI/Engine。使用 JDK 17、Scala 2.13、sbt 1.10.x 构建。

Maven 制品名和可执行程序均为 `dacp-operator-repository`（不带 Scala 后缀），源码包名为 `dacp.repository`。

## 初始目录

在项目根目录执行：

```bash
scripts/setup-operator-repository.sh --root .operator-catalog --host 0.0.0.0 --port 8820
curl http://localhost:8820/v1/operators
```

脚本构建共享环境 `dacp/operator-jvm:1.1` 和可选的完整镜像 `dacp/format-convert:1.1`，打包仓库，发布初始算子并启动服务。需要 Linux、带 `jlink` 的 JDK 17、sbt、Python 3、`ldd`、Docker Engine 访问权限，以及首次构建时的 Maven 网络访问。默认目录是项目根目录的 `.operator-catalog`。重复执行会验证已有版本与共享制品，不覆盖版本。其余参数传给 `serve`，包括 HTTPS 参数。

| 算子 | 用途 |
|---|---|
| `format_convert@1.1` | CSV/NDJSON 转 Parquet；参见 [参数](../docs/reference.md#format_convert-参数) |
| `identity@1.1` | 复制原始字节，用于传输检查和流水线 |

两者共享薄 Scala JAR 和运行时环境，仓库只导出一次运行时。详见 [运行时构建](../dacp-operator-runtime/README.md)、[用户自定义 Scala 示例](../examples/operators/scala/README.md)、[Python 示例](../examples/operators/python/README.md) 和 [打包契约](../docs/operator-packaging.md)。

## 发布与执行

```bash
# 在项目根目录执行；相关镜像必须已安装。
(cd dacp-operator-repository && sbt test stage)
python3 scripts/package-operator-sources.py
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`seed` 发现 `<operators>/<name>/<version>/operator.yaml`。必填 `name`、`version`、`image`、`inputs`、`outputs`；可选 `description`、`executor`（默认 `container`）、`entrypoint`、`artifact`（相对文件路径）、`executor_config`、`provenance`。名称和版本匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。未知字段、重复键、目录身份不符和空定义目录会失败。

手动发布薄 JAR：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@1 --root .operator-catalog --image example/jvm-arrow:1 --executor jvm --entrypoint example.Summary --artifact summary.jar --ports summary-ports.yaml --description "Summarize records"
```

Python 使用 `--executor python --entrypoint my_operators.summary --artifact my_operators-1.0-py3-none-any.whl`，或 `.py` 脚本。依赖应放入共享运行时。完整容器省略 `--executor`、`--entrypoint`、`--artifact`，由镜像入口实现 [Arrow IPC 契约](../docs/reference.md#任务执行与算子)。

发布固定镜像 ID，每个镜像只导出一次，按摘要存储制品并原子发布元数据；文件锁协调发布者。新名称和主类可共享一个代码包，无需修改服务端。仓库可存储任何合法执行器 ID，节点须安装对应启动插件。内置 `container`、`jvm`、`python` 与外部插件使用相同 ServiceLoader 契约。

配置每个执行节点：

```yaml
local_operator_library: ../operators
remote_operator_library_urls:
  - http://library-host:8820
  - https://another-library.example.org
operator_cache: /var/lib/dacp/operators
operator_max_bytes: 8589934592
```

任务继续使用 `operator: format_convert@1.1`。解析顺序为本地定义与已安装镜像、完整缓存、配置的远程仓库。仅清单 HTTP 404 会继续下一个仓库。远程与缓存制品均验证大小和 SHA-256，执行使用固定镜像 ID。制品可跨算子和仓库共享，完整缓存支持离线执行。大小限制针对单个制品。参见 [缓存布局与限制](../docs/operator-packaging.md#content-addressed-storage)。

## 公开网站与 HTTP API

打开 `http://localhost:8820/`，无需登录即可搜索名称、版本和描述，查看版本并下载代码和运行时。页面分别展示大小、校验和、执行器、入口、运行时镜像 ID 及任务引用。所有文本转义，无需 JavaScript 或外部资源。发布支持管理员 CLI 和下述 token 认证 HTTP API。

| GET / HEAD 端点 | 响应 |
|---|---|
| `/` 或 `/?q=keyword` | 公开目录，忽略大小写搜索，最多 200 字符 |
| `/operators/<name>/<version>` | 详情与分别下载制品 |
| `/assets/repository.css` | 本地样式 |
| `/v1/operators` | 按名称/版本排序的 JSON 清单数组 |
| `/v1/operators/<name>/<version>` | 清单 JSON，格式为 `api_version: 2` |
| `/v1/blobs/sha256/<64 lowercase hex digits>` | 共享运行时归档或代码内容 |
| `/health` | `ok` 加换行 |

未知路径或缺失制品返回 404，未启用或不支持的写方法返回 405。HEAD 返回 Content-Length。网页附件使用制品原文件名和后缀。路径不能沿符号链接越出目录。8 个工作线程使用有界队列。列表忽略发布暂存目录，只读元数据而不对大制品计算摘要。

元数据位于 `<root>/<name>/<version>/manifest.json`，制品位于 `<root>/blobs/sha256/<digest>`，运行时导出索引位于 `<root>/images/<image-ID>.json`。参见 [清单格式](../docs/operator-packaging.md#content-addressed-storage)。项目尚未发布，直接采用新格式，不保留旧格式读取器；新格式使用新的开发目录。

## Token 认证发布 API

管理员通过 `--publish-token-env` 指定保存发布 token 的环境变量名；token 至少 32 个可见 ASCII 字符。不配置此选项时发布接口关闭。缺失或错误 token 返回 401，公开浏览与下载不要求 token。一个 token 授予整个仓库的发布权限；更换环境变量后重启服务即可轮换，不会改变已发布算子。

```bash
export DACP_REPOSITORY_PUBLISH_TOKEN="$(openssl rand -hex 32)"
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 127.0.0.1 --port 8820 --publish-token-env DACP_REPOSITORY_PUBLISH_TOKEN --publish-max-bytes 8589934592
```

跨机器发布使用 HTTPS，可通过下节的内置 TLS 或可信反向代理提供。token 放在 `Authorization: Bearer <token>` 请求头，不放在 URL 中。网站仍公开只读，发布通过 HTTP API 或上传脚本完成。

| 接口 | 请求及行为 |
|---|---|
| `PUT /v1/blobs/sha256/<digest>` | `application/octet-stream` 原始文件；流式接收并验证 SHA-256，重复上传相同内容可复用 |
| `POST /v1/operators` | `application/json`，现有 `api_version: 2` 清单；引用的所有制品须先上传并通过大小、摘要与元数据校验 |

成功返回 201；清单响应的 `Location` 指向公开清单。已有同名同版本返回 409，不覆盖内容。错误摘要或无效清单返回 400，不支持的 Content-Type 返回 415，超限返回 413。清单上限 64 KiB；单次制品默认上限 8 GiB，由 `--publish-max-bytes` 设置。完整制品上传后才写入内容寻址目录，清单验证成功后原子发布；失败的临时上传会删除。已上传但未被清单引用的制品会保留，当前没有自动回收机制。

上传在发布者本机通过 CLI 生成的目录（`publish` 或 `seed`），包含运行时、代码和源码材料：

```bash
python3 scripts/upload-operator.py summary@1 --catalog .operator-catalog --url https://repository.example.org --token-env DACP_REPOSITORY_PUBLISH_TOKEN
```

脚本仅依赖 Python 标准库，逐块上传大文件，自动收集清单中的运行时、算子代码、源码归档、许可证文件和 SBOM。本机测试可使用 `--url http://localhost:8820`。仓库服务器不执行上传代码、不拉取外部 URL、也不调用 Docker；导出 Docker 运行时发生在发布者本机。源码和许可证材料仍由发布者声明，不构成安全或合法性认证。

## HTTPS 与运行

`serve` 默认 `127.0.0.1:8820`，外部访问使用 `--host 0.0.0.0`。必须设置 `--root`。打包程序无需 sbt 即可运行，Ctrl-C 或 SIGTERM 停止。目录文件系统须支持文件锁及原子移动，写权限仅授予管理员。

```bash
export DACP_REPOSITORY_TLS_PASSWORD='your-keystore-password'
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820 --tls-keystore repository.p12 --tls-password-env DACP_REPOSITORY_TLS_PASSWORD
```

TLS 两个选项须同时提供，节点 JVM 信任库须信任证书；也可由反向代理终止 HTTPS。SHA-256 校验完整性，HTTPS 验证来源。数据集和任务继续使用 Flight mTLS。缓存与流资源生命周期由执行节点负责，目前制品缓存无自动垃圾回收。

## 验证

```bash
(cd dacp-operator-repository && sbt test)
(cd dacp-operator-executors && sbt test)
scripts/test-operator-repository.sh
```

单元及 HTTP 测试使用注入的导出器，无需 Docker。联调需要已构建运行时、Python SDK、sbt、Docker 和空闲示例端口；启动仓库与两个使用独立缓存的 mTLS 节点，执行分布式转换/identity DAG。日志和制品保留在输出的临时目录，服务在退出时停止。

完整的编写与上传教程（Scala/Python 源码、wheel 打包、远程文件传输及任务执行）见 [自定义算子指南](../docs/operator-authoring.md)。

定义必须包含命名 `inputs`、`outputs` 端口。`publish --ports FILE` 为必填项，文件可为包含端口声明的 YAML/JSON 或完整算子定义；`seed` 从定义中读取这些声明。详见[编写指南](../docs/operator-authoring.md)。

原生命令使用 `executor: command`，插件设置位于 `executor_config`。`seed` 保留该映射；手工发布用 `--executor-config FILE` 传入单独的 YAML/JSON 映射，仓库保存为 `execution.config`，执行节点插件验证。详见[原生命令指南](../docs/native-commands.md)。

## 算子审查材料

支持发布可选的源码包、仓库修订版本、许可证声明与文本、构建说明和 SBOM。公开页面提供下载并标注缺失信息。参见[源码与审查信息](../docs/operator-provenance.md)。

首页按算子名称合并，每个算子只展示最新版本，计数按算子计算。版本标签中的数字段按整数比较（如 `1.10` 高于 `1.9`），正式版本高于同名带连字符的预发布版本，其他文本按字典序比较。搜索匹配任一版本的名称、版本或描述时，仍展示该算子的最新版本。详情页按版本从新到旧列出全部版本；JSON 目录仍包含全部版本。

网页下载链接在摘要后携带原文件名：`/v1/blobs/sha256/DIGEST/FILENAME`。GET 和 HEAD 响应通过 `Content-Disposition` 保留原文件名及后缀。仅含摘要的 URL 用于按内容寻址的读取 API。

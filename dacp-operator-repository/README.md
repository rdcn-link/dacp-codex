# DACP 算子仓库

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
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators operators --root .operator-catalog
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`seed` 发现 `<operators>/<name>/<version>/operator.yaml`。必填 `name`、`version`、`image`、`inputs`、`outputs`；可选 `description`、`executor`（默认 `container`）、`entrypoint`、`artifact`（相对文件路径）。名称和版本匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。未知字段、重复键、目录身份不符和空定义目录会失败。

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

打开 `http://localhost:8820/`，无需登录即可搜索名称、版本和描述，查看版本并下载代码和运行时。页面分别展示大小、校验和、执行器、入口、运行时镜像 ID 及任务引用。所有文本转义，无需 JavaScript 或外部资源。发布仅由管理员 CLI 完成，没有 HTTP 上传接口。

| GET / HEAD 端点 | 响应 |
|---|---|
| `/` 或 `/?q=keyword` | 公开目录，忽略大小写搜索，最多 200 字符 |
| `/operators/<name>/<version>` | 详情与分别下载制品 |
| `/assets/repository.css` | 本地样式 |
| `/v1/operators` | 按名称/版本排序的 JSON 清单数组 |
| `/v1/operators/<name>/<version>` | 清单 JSON，格式为 `api_version: 2` |
| `/v1/blobs/sha256/<64 lowercase hex digits>` | 共享运行时归档或代码内容 |
| `/health` | `ok` 加换行 |

未知路径或缺失制品返回 404，写方法返回 405。HEAD 返回 Content-Length。HTTP 附件文件名为摘要，HTML download 属性建议使用制品文件名。路径不能沿符号链接越出目录。8 个工作线程使用有界队列。列表忽略发布暂存目录，只读元数据而不对大制品计算摘要。

元数据位于 `<root>/<name>/<version>/manifest.json`，制品位于 `<root>/blobs/sha256/<digest>`，运行时导出索引位于 `<root>/images/<image-ID>.json`。参见 [清单格式](../docs/operator-packaging.md#content-addressed-storage)。项目尚未发布，直接采用新格式，不保留旧格式读取器；新格式使用新的开发目录。

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

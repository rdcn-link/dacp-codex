# DACP 算子库

独立的 Scala 模块，通过 HTTP 或 HTTPS 分发不可变的 `name@version` 算子清单和 Docker 镜像归档。需要 JDK 17、Scala 2.13 和 sbt 1.10.x。仓库独立于 DACP 数据服务器运行；提供已发布文件不需要 Docker，发布需要 Docker CLI 和引擎访问权限。

## 初始算子目录

从项目根目录准备并启动一个已包含算子的仓库：

```bash
scripts/setup-operator-library.sh --root .operator-catalog --host 0.0.0.0 --port 8820
curl http://localhost:8820/v1/operators
```

该命令构建共享 Docker 镜像、打包 Scala 服务、初始化算子目录并启动服务。需要 Docker 引擎访问权限、sbt 和构建时的网络访问权限。重复执行时会校验已有发布版本并保持其不可变；损坏文件会终止初始化。默认仓库为项目根目录下的 `.operator-catalog`。其他参数传递给 `serve`，包括 HTTPS 选项。

| 初始算子 | 用途 | 参数 |
|---|---|---|
| `format_convert@1.0` | 将 CSV 或 NDJSON Item 转为 Parquet，保留独立 Item | `from: auto`、`to: parquet`、`schema: infer`；参见[转换参考](../docs/reference.md) |
| `identity@1.0` | 将输入字节流复制到新数据集，可用于检查传输和任务串联 | 无 |

两个算子均使用 `dacp/format-convert:1.0` 镜像。算子可使用 Python、Scala 或其他语言实现，其容器必须遵守 [Arrow IPC 契约](../docs/reference.md)。

对于已构建的部署，可以仅初始化目录而不启动服务：

```bash
cd dacp-operator-library
target/universal/stage/bin/dacp-operator-library seed --operators ../operators --root .operator-catalog
```

`seed` 发现 `<operators>/<name>/<version>/operator.yaml` 中包含 `name`、`version` 和 `image` 的定义，引用的所有镜像必须已在本地安装。贡献新算子只需添加定义并构建镜像，无需修改注册表。定义目录为空或身份无效时明确报错。即使源镜像标签发生变化，已有版本也保持不变；代码更新必须发布新版本。

## 手动配置

从项目根目录构建并启动：

```bash
cd dacp-operator-library
sbt test stage
# 请先构建算子镜像，参见项目 README。
target/universal/stage/bin/dacp-operator-library publish format_convert@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
target/universal/stage/bin/dacp-operator-library publish identity@1.0 --image dacp/format-convert:1.0 --root .operator-catalog
target/universal/stage/bin/dacp-operator-library serve --root .operator-catalog --host 0.0.0.0 --port 8820
```

`serve` 默认监听 `127.0.0.1:8820`。两个命令都必须提供 `--root`。打包后的启动器无需 sbt 即可运行。通过 SIGTERM 或 Ctrl-C 停止服务。

为每个执行节点配置：

```yaml
local_operator_library: ../operators
remote_operator_library_urls:
  - http://library-host:8820
  - https://another-library.example.org
operator_cache: /var/lib/dacp/operators
operator_max_bytes: 8589934592
```

任务使用 `operator: format_convert@1.0` 引用算子。解析首先检查本地定义及已安装的 Docker 镜像，然后检查已完成的下载缓存，最后依次查询远程库。清单缺失（HTTP 404）时继续下一个库。其他 HTTP 错误、无效清单或损坏归档会使任务失败。远程库离线时仍可使用已完成的缓存。每个执行节点在将镜像加载到其 Docker 引擎前，校验算子身份、镜像 ID、归档大小和 SHA-256。执行使用固定的镜像 ID。

## 发布与 HTTP 契约

按照 [Arrow IPC 容器契约](../docs/reference.md) 实现算子，构建 Docker 镜像，再运行 `publish name@version --image IMAGE --root DIR` 发布。无需修改服务器注册表或客户端。名称和版本必须匹配 `[A-Za-z0-9_-][A-Za-z0-9_.-]*`。

发布时检查镜像 ID，通过 `docker save` 导出该固定 ID，以有界内存计算 SHA-256，并原子发布完整目录。文件锁协调发布进程；已有版本不能覆盖。导出失败会删除临时文件。仓库目录所在文件系统必须支持原子目录移动和文件锁，且仅应允许管理员写入。

| GET 端点 | 响应 |
|---|---|
| `/v1/operators` | `200`，按名称和版本排序的已发布清单 JSON 数组，不包含未完成的临时目录 |
| `/health` | `200`，`ok` 后跟换行符 |
| `/v1/operators/<name>/<version>` | `200`，JSON 清单 |
| `/v1/operators/<name>/<version>/image.tar` | `200`，以有界内存流式传输 Docker save 归档 |

未知路径和缺失文件返回 404。其他 HTTP 方法返回 405。不提供 HTTP 上传端点。请求不能通过符号链接访问仓库外部。服务使用八个请求工作线程和有界队列。

磁盘布局为 `<root>/<name>/<version>/{manifest.json,image.tar}`。清单契约如下：

```json
{"api_version":1,"name":"identity","version":"1.0","image":"sha256:<64 lowercase hex digits>","archive":{"size":12345,"sha256":"<64 lowercase hex digits>"}}
```

下载超时、大小限制、缓存校验和生命周期由执行节点负责。缓存存储与任务输出 TTL 分离。损坏缓存会明确报错；删除对应版本缓存目录后可重新下载。

## HTTPS

使用包含服务器证书和私钥的 PKCS12 密钥库，通过环境变量提供密码：

```bash
export DACP_LIBRARY_TLS_PASSWORD='your-keystore-password'
target/universal/stage/bin/dacp-operator-library serve --root .operator-catalog --host 0.0.0.0 --port 8820 --tls-keystore library.p12 --tls-password-env DACP_LIBRARY_TLS_PASSWORD
```

两个 TLS 选项必须同时提供。执行节点应配置 HTTPS URL，并使用信任该证书的 JVM 信任库。HTTP 用于可信开发网络。SHA-256 校验归档完整性，HTTPS 确立清单来源。任务和数据通信继续使用 Flight mTLS 和 URI 授权。

## 验证

```bash
cd dacp-operator-library
sbt test
cd ..
scripts/test-operator-library.sh
```

单元和 HTTP 契约测试使用注入的镜像导出器，不需要 Docker 引擎。集成脚本需要 Python SDK、sbt、Docker 访问权限和已构建的 `dacp/format-convert:1.0` 镜像。脚本打包 Scala 仓库，发布两个算子，启动算子库和两个使用独立缓存的 mTLS 节点，再运行分布式转换与 identity DAG。大型归档需要数 GB 可用空间。日志、仓库和缓存保留在打印的临时目录中，退出时停止服务。

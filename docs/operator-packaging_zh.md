# 算子打包与共享运行环境

算子必须在 `inputs` 之外声明静态 `parameters` 契约，详见[参数类型与表单](operator-parameters_zh.md)。输出结构仍只由推导方法给出。
输出结构统一由必选推导方法提供，清单声明输入与参数契约；详见[结构推导](structure-inference_zh.md)。

算子由不可变的 `name@version` 清单、共享运行环境镜像和可选的小型代码文件组成。多个名称和版本可以引用相同运行环境及代码包。已支持完整容器、精简 Java/Scala JAR、Python 源文件和 wheel。算子始终在独立 Docker 容器内运行，公共仓库文件不加载到服务器 JVM。

## 运行环境和依赖

将依赖预先构建到有版本的运行镜像中。清单固定 Docker image ID，以及 Docker-save 归档的 SHA-256 和大小。执行任务时不从互联网安装依赖。依赖集合变化产生新镜像；仅代码变化可复用已有运行环境描述。镜像依赖平台，需匹配执行节点 OS/架构，尚未实现自动平台选择。

JVM 环境提供 `/opt/java/bin/java` 和 `/opt/lib/*` 中的依赖；Python 环境提供 `/usr/local/bin/python`、pip 和全部依赖。wheel 通过 `--no-index --no-deps` 离线安装到临时目录，pip 校验平台/Python 标签；单个 `.py` 无需安装。多模块和原生扩展优先使用 wheel；原生库需放入平台兼容 wheel 或共享环境。源码支持单脚本，不自动解析依赖。

一个包可包含多个 main 类或 Python 模块。每个算子发布独立入口，复用同一 artifact。依赖和发布节奏相同的算子可合包；独立代码文件仍可共享运行环境。Java/Scala 使用相同 `jvm` 执行器。支持 fat JAR，但会重复依赖，精简 JAR 的增量下载更小。

## 定义和发布

定义位于 `<operators>/<name>/<version>/operator.yaml`。必需字段为 `name`、`version`、`image`、`icon`、`inputs`、`parameters`；可选字段包括 `description`、`executor`（默认 `container`）、`entrypoint`、`artifact` 及插件定义的 `executor_config`。artifact 相对于定义文件。拒绝未知字段和重复 YAML 键。

```yaml
name: summary
version: "1"
image: example/jvm-arrow:1
executor: jvm
entrypoint: example.Summary
artifact: summary.jar
inputs:
  source: {representations: [records], formats: []}
icon:
  path: "M3 5h18v14H3zM3 10h18M9 5v14"
  color: "#14866b"
parameters:
  type: object
  properties: {}
  required: []
  additionalProperties: false
```

`jvm` 需要 JAR 和 main 类。`python` 需要 `.py` 或 `.whl`，wheel 用 `entrypoint` 选择模块，源码直接执行脚本。`command` 适配原生 stdin/stdout 程序，见[原生命令](native-commands_zh.md)。`container` 使用镜像自带入口，不接受入口覆盖或代码文件。每个执行器接收 `operator /input /input/params.json` 和 `DACP_OPERATOR=name@version`，遵守 [Arrow IPC 契约](reference_zh.md#任务执行与算子)。

```bash
# 在完成仓库 stage 构建后，从项目根目录运行。
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@1 --root .operator-catalog --image example/jvm-arrow:1 --executor jvm --entrypoint example.Summary --artifact summary.jar --ports ports.yaml
```

仓库存储和提供清单不需要执行器插件。执行节点必须安装对应插件。发布校验保持通用，入口和 artifact 的执行器专用校验在执行节点进行。

## 内容寻址存储

`api_version: 2` 清单分别描述执行、运行环境和代码：

```json
{
  "api_version": 2,
  "name": "summary",
  "version": "1",
  "description": "Summarize records",
  "inputs": {"source": {"representations": ["records"], "formats": []}},
  "execution": {"executor": "jvm", "entrypoint": "example.Summary"},
  "runtime": {
    "image": "sha256:<image ID>",
    "archive": {"sha256": "<archive digest>", "size": 123456, "filename": "runtime.tar"}
  },
  "artifact": {"sha256": "<code digest>", "size": 1234, "filename": "summary.jar"}
}
```

完整容器省略 `artifact`。摘要为 64 位小写十六进制，大小为正字节数。元数据原子发布，版本不能覆盖。项目直接使用此 schema，不提供旧清单读取或迁移回退。

仓库布局：

```text
<root>/<name>/<version>/manifest.json
<root>/blobs/sha256/<digest>
<root>/images/<image-ID-without-prefix>.json
```

镜像索引避免对同一镜像重复 `docker save`。代码和镜像归档按摘要只存一份。公共 `/v1/blobs/sha256/<digest>` 提供字节；算子页面分别提供运行环境和代码下载、大小及校验和。

执行节点布局：

```text
<operator_cache>/manifests/<repository-URL-hash>/<name>/<version>.json
<operator_cache>/blobs/sha256/<digest>
```

元数据按仓库 URL 隔离；校验后的 blob 在名称、版本和仓库之间共享。锁保护并发发布/下载，原子移动防止不完整缓存。执行前校验远程和缓存文件；损坏 blob 明确失败，可删除后重下。`operator_max_bytes` 对单个文件生效，不是总量或容器解压大小。缓存暂不自动回收。

本地定义和已安装镜像优先。先尝试完整缓存清单，再按配置顺序请求远程仓库。只有清单 404 才尝试下一仓库；校验和、HTTP 和验证错误使任务失败。缓存清单及 blob 支持离线执行；Docker 中缺少固定 image ID 时才加载镜像。

Docker 层在本地共享，JVM 构建器把运行环境层与可选完整镜像代码层分开。当前仓库传输完整内容寻址镜像归档；OCI 按层传输及不同运行环境之间的层传输去重尚未实现。

## 执行器插件

实现 `dacp-plugin-api` 的 `dacp.OperatorExecutor`，在 `META-INF/services/dacp.OperatorExecutor` 注册完整类名。API 依赖为 `Provided`，插件和私有依赖放到 `plugin_directory` 后重启。无需修改核心注册表或客户端。重复执行器 ID 明确失败。内置 `container`、`jvm`、`python`、`command` 位于 `plugins/dacp-plugin-operator-executors`，使用相同 ServiceLoader 机制。

```scala
final class CustomExecutor extends dacp.OperatorExecutor {
  val id = "custom"
  def command(p: dacp.OperatorProgram, artifactPath: Option[String]): dacp.OperatorCommand = {
    require(p.entrypoint.nonEmpty, "entrypoint required")
    dacp.OperatorCommand(entrypoint = Some(Vector("/opt/custom/bin/run", p.entrypoint)))
  }
}
```

插件校验入口、artifact 和 `OperatorProgram.configuration`，提供容器命令和环境。`OperatorProgram.artifact` 是校验后的宿主路径，`artifactPath` 是容器内只读路径。插件是可信启动代码，算子文件是独立可执行载荷。服务器负责镜像加载、只读挂载/根文件系统、禁用网络、临时空间、内存/PID 限制、超时、清理、Flight 和流交付。插件不替代隔离和资源生命周期，没有宿主进程执行器。

## 验证

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
(cd dacp-operator-repository && sbt test)
(cd dacp-server && sbt 'testOnly dacp.OperatorLibrarySpec dacp.CoreSpec dacp.CollaborationSpec')
scripts/test-operator-repository.sh
```

契约测试覆盖服务注册、启动校验、共享导出/代码包/下载、不可变发布、公共下载、校验失败、清理、仓库顺序和离线缓存。仓库集成测试通过分布式 Arrow/FIFO 流水线执行真实 JAR。[Scala 示例](../examples/operators/scala/README_zh.md) 独立编译并使用运行环境提供的依赖；[Python 示例](../examples/operators/python/README_zh.md) 演示源码和 wheel。

完整源码、步骤及远程上传见[用户自定义算子指南](operator-authoring_zh.md)。

## 执行契约

[基于 Item 的纯流式契约](operator-streams_zh.md) 绑定命名 Item 输入并返回命名 Item 流。发布所用 `ports.yaml` 的 `inputs`、`outputs` 与本地定义相同。Dataset 映射和通用流水线调度尚未实现。

## 可选审查材料

API v2 支持可选 `provenance` 元数据，以及 SHA-256 寻址的源码、许可证和 SBOM 下载。这些文件与执行分离，执行节点不自动获取。YAML 字段、CLI 参数、清单描述和校验限制见[源码与审查信息](operator-provenance_zh.md)。

算子还必须提供 `icon`；公开 `categories` 由管理员指定，规则和发布示例见[分类与图标](operator-presentation_zh.md)。

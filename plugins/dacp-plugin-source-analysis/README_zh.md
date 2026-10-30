# 源码分析与构建插件

[English](README.md)

提供仓库的有限源码扫描、安全 ZIP/TAR 解包与 Python/Java/Scala 构建模板，不在宿主执行上传程序。参见[提交及隔离限制](../../docs/operator-submission_zh.md)。

## 扩展契约

公开契约位于 `dacp-operator-sdk/contract`：

- `dacp.operator.SourceAnalyzer`：唯一分析器 `id`、`supports(filename)`，`analyze(Path, filename)` 返回包含 `status`（`complete` 或 `failed`）、`score`（完成时为 0–5）、发现和覆盖情况的报告。仓库运行全部已发现分析器，采用最低完成得分；任意分析器失败则不能批准。
- `dacp.operator.SourceArchive`：实现 `supports(filename)` 和 `unpack(archive, filename, destination)`，检查路径、链接、类型、压缩/解压大小与文件数，不得写出目标目录。内置实现支持 ZIP、TAR、TAR.GZ、TGZ。
- `dacp.operator.SourceBuildRecipe`：语言、执行器、产物名、固定命令及产物入口校验。命令在受限容器中运行，可访问 `/source`、`/tmp`、`/output` 与受信任的 `/builder/helper.jar`。stdout 仅写产物，诊断写 stderr。依赖必须由已注册镜像提供。

外部插件依赖 `dacp-operator-sdk/contract`，提供公开无参实现类，并在 `META-INF/services/<契约完整类名>` 中逐行注册类名。将插件 JAR 及依赖放入仓库启动器类路径，例如分发包的 `lib/`，重启即可，不需要修改核心注册表、重建服务器或改变客户端。归档第三方依赖保留在本插件模块。构建语言重复会导致初始化失败，每种语言只提供一个模板。该扩展点已实现；语义级 SAST、依赖漏洞扫描及任意构建系统尚未实现。

例如，在 `META-INF/services/dacp.operator.SourceAnalyzer` 注册 `example.CompanyAnalyzer`。不支持或失败的分析应明确报告，不能给出无风险分数。分析器是由管理员安装的受信任代码，应限制 CPU、内存、文件读取和报告大小。

## 契约测试

```bash
cd plugins/dacp-plugin-source-analysis
sbt test
```

测试包含正常源码、网络/删除证据、不完整覆盖、路径穿越和解压限制。仓库测试覆盖仅源码提交、私有文件、异步分析/构建状态、人工审核、公开可信度及分类、提交者隔离与 Token 撤销。新增插件应提供针对异常输入、覆盖缺口和资源上限的同等测试。

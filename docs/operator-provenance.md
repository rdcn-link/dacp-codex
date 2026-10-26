# 算子的源码与审查信息

发布者可以为算子附加可选的审查材料。公开详情页展示源码是否可用、源码仓库与修订版本、声明的许可证、构建说明和下载链接，并明确标注缺失的源码、修订版本、许可证或说明。源码可用和 SHA-256 校验不能证明程序安全、法律合规，也不能证明执行文件由所附源码构建。

在 `operator.yaml` 中添加可选配置：

```yaml
provenance:
  source_archive: sources.zip
  source_repository: https://example.org/team/operator
  source_revision: <exact-commit>
  license: MIT
  license_file: LICENSE.txt
  build_instructions: |
    javac -d classes src/example/Summary.java
    jar --create --file summary.jar -C classes .
  sbom: dependencies.cdx.json
```

所有字段均可省略，无法证实的信息请不要填写。文件路径相对于 `operator.yaml`。源码包必须为非空 ZIP、TAR、TAR.GZ 或 TGZ 文件；许可证与 SBOM 必须为非空文件。仓库仅存储文件字节，不解压源码、不验证许可证、不解析 SBOM。建议采用 SPDX 许可证标识或表达式，以及 CycloneDX 或 SPDX SBOM。源码包应包含源代码、依赖声明、构建工具配置及可复现的构建步骤。修订版本必须同时提供仓库 URL；该信息由发布者声明，仓库不会核对源码包与修订版本。构建说明最多为 32 KiB UTF-8，完整清单最多为 64 KiB。

管理员命令示例（在项目根目录运行）：

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish summary@2 \
  --root .operator-catalog --image example/jvm-arrow:1 \
  --executor jvm --entrypoint example.Summary --artifact summary.jar \
  --ports summary-ports.yaml \
  --source-archive sources.zip \
  --source-repository https://example.org/team/operator \
  --source-revision 'REPLACE_WITH_EXACT_COMMIT' \
  --license MIT --license-file LICENSE.txt \
  --build-instructions BUILD.txt --sbom dependencies.cdx.json
```

运行前替换 `<exact-commit>`。YAML 中直接填写构建说明，而 `--build-instructions` 读取 UTF-8 文本文件。发布支持管理员 CLI 或 [token 认证 HTTP API](../dacp-operator-repository/README.md#token-认证发布-api)，公开浏览与下载不需要 token。已发布版本不能修改，补充审查材料时须发布新版本。

API v2 清单增加可选的 `provenance` 对象。文本字段保持上述名称；文件字段变为 `{ "sha256": "<64 lowercase hex digits>", "size": 123, "filename": "sources.zip" }`。通过 `/v1/blobs/sha256/<sha256>` 下载，并核对大小和 SHA-256。发布和重复初始化会校验材料文件；源码、许可证和 SBOM 与执行文件共用内容寻址存储。

执行节点只获取执行文件和运行环境。审查材料由读者主动下载，不会执行或自动安装。信任算子前应审查源码、依赖、许可证及构建流程；附加材料属于发布者声明。

## 附带源码的示例

初始化新示例前，在项目根目录运行 `python3 scripts/package-operator-sources.py`。脚本从当前非忽略的 Git 工作区文件生成 `.operator-sources/dacp-sources.zip`，包含未提交修改，ZIP 时间戳固定。发布前请检查文件内容；这是工作区快照，并非经过认证的提交导出。项目当前没有许可证声明，因此示例不填写许可证，也不虚构 SBOM。

可构建并发布的源码示例为 `identity@1.3`、`format_convert@1.3`、`identity_python@1.1`、`native_uppercase@1.1`。按现有示例说明构建 JVM JAR、安装共享运行环境镜像并编译原生程序，然后打包源码，分别初始化 `operators`、`examples/operators/python` 和 `examples/operators/native/operators`。标准启动脚本会自动打包源码并初始化 JVM 示例。新示例提供快照下载和构建说明；这些源码示例不代表当前在线仓库的发布清单；实际可用算子见[协作算子库](operator-catalog.md)。

集成验证：先在专用测试仓库 `localhost:8820` 发布上述示例，已构建 `dacp-server`、准备示例 mTLS 证书，并在 `.venv` 安装 Python SDK/PyArrow 且具备 Docker 权限后，运行 `.venv/bin/python scripts/test-operator-provenance.py`。脚本自动启动和停止临时执行节点，检查 JVM/Python/原生流与 Parquet 转换，校验审查材料下载与网页，并断言执行时不会下载源码材料。

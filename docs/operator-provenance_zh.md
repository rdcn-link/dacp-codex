# 算子源码与审核材料

[English](operator-provenance.md)

公开仓库的提交必须包含源码。开发者声明 `source_public: true` 或 `false`，管理员始终可以认证访问。仓库先扫描、构建，再进行审核。批准后的公开清单仅在声明公开时包含源码；源码私有时，源码仓库/版本与构建说明也保持私有。许可证和 SBOM 在批准后公开。

[源码提交 YAML](operator-submission_zh.md)通过 `--source sources.zip` 提供源码，可选补充材料如下：

```yaml
source_public: false
provenance:
  source_repository: https://example.org/team/operator
  source_revision: exact-commit
  license: Apache-2.0
  license_file: LICENSE.txt
  build_instructions: |
    Uses the registered python-arrow@1.0 environment.
    No additional dependencies are downloaded.
  sbom: dependencies.cdx.json
```

路径相对提交 YAML 解析。`source_revision` 要求提供 `source_repository`。版本和许可证是提交者声明，不是自动核验的证明。许可证/SBOM 作为不透明审核材料保留。构建说明是供审核阅读的文本，绝不执行；实际构建使用仓库固定模板与管理员环境。构建说明最多 32 KiB UTF-8，完整 HTTP 清单最多 64 KiB。

文件描述包含 `sha256`、`size`、`filename`。允许公开的文件通过 `/v1/blobs/sha256/{hash}/{filename}` 下载。私有源码和待审产物必须通过管理员认证接口访问，仅知道摘要不能公开下载。管理员查看原始扫描发现、覆盖情况与分数，再指定公开的最终可信度。自动分析可能漏报，分数不是安全或合法性证明。

执行节点下载执行产物与运行环境，不下载审核归档。完整[提交、构建与审核说明](operator-submission_zh.md)包含接口示例、归档限制、分析器局限、隔离与私有源码行为。

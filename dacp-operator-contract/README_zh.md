# DACP 算子元信息契约

[English](README.md)

轻量 Scala 模块，只依赖 Jackson，供算子仓库、DACP 节点和设计器共享静态参数定义验证。`dacp.operator.ParameterSchema.validate(JsonNode)` 检查 `parameters` 的递归结构、类型、默认值和约束；不执行算子、不填充 Job 默认参数。

契约与配置示例见[算子参数契约](../docs/operator-parameters_zh.md)。它适用于所有执行器及第三方算子，无需核心注册。

```bash
cd dacp-operator-contract
sbt test
```

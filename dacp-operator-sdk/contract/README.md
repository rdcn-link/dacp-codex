# DACP operator metadata contracts

[中文](README_zh.md)

A lightweight Scala module depending only on Jackson. The operator repository, DACP nodes, and designer share static parameter-definition validation. `dacp.operator.ParameterSchema.validate(JsonNode)` checks recursive structure, types, defaults, and constraints in `parameters`; it neither executes operators nor fills Job parameter defaults.

See [operator parameter contracts](../../docs/operator-parameters.md) for the contract and configuration examples. It applies to all executors and third-party operators without core registration.

```bash
cd dacp-operator-sdk/contract
sbt test
```

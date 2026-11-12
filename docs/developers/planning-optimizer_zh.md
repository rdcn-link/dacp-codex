# 扩展物理计划优化

实现由 `dacp-union-protocol` 提供的 `dacp.union.PhysicalPlanOptimizer`。内置 `cost` 和外部 provider 使用相同的 `PluginEnvironment` 发现及生命周期。模型与限制参见[物理计划](../physical-planning_zh.md)。

## 契约

| 方法 | 用途 | 限制 |
|---|---|---|
| `validate(settings)` | 校验插件拥有的启动参数 | 拒绝未知／无效值 |
| `estimate(input, step, node, inputBytes, settings)` | 可选的输出大小、计算／启动耗时、内存估算 | 有限非负，不读取源数据 |
| `link(input, from, to, settings)` | 可选的带宽、延迟估算 | 带宽有限且大于零，延迟非负 |
| `candidates(input, settings)` | 额外的完整步骤到节点分配 | 最多 256 个，步骤标识完整且节点已批准 |
| `score(plan, settings)` | 搜索最小化的目标值 | 有限非负 |

估算／链路钩子返回 `None` 时使用基础模型。`PlanningInput` 包含不可变步骤、来源大小、算子引用、序列化参数、逐节点已安装画像和目标节点。画像中的 fusion key 仅供估算，仍需 executor 验证。节点 ID 为证书身份，不是展示名称。参数可能包含敏感信息，不应完整写入日志。

回调会对部分和完整候选重复调用，必须确定、无副作用、快速、有界且线程安全；评分时不要执行算子或获取数据。资源在 `initialize` 获取，在 `close` 释放。插件异常或无效返回值会使规划失败，不会静默改变策略。候选搜索会持续检查服务端取消。插件属于进程内可信代码，必须配合取消和资源约束，不受沙箱隔离。

## 安装 provider

使用 Scala 2.13 编译，依赖 `io.github.rdcn-link:dacp-union-protocol_2.13:0.1.0-SNAPSHOT`，采用 `Provided` scope。发布制品可用前，从本地构建获取与宿主匹配的 SDK。不要打入重复公共 API。将实现类完整名称写入：

```text
META-INF/services/dacp.union.PhysicalPlanOptimizer
```

将 JAR 与其私有依赖放入 Union 的 `plugin_directory`，选择 provider 并重启 Union：

```yaml
plugin_directory: /opt/dacp/union-plugins
planning:
  provider: transfer-aware
  beam_width: 32
  settings:
    transfer_penalty_ms_per_mb: 10
```

[可运行 Scala 示例](../../examples/planning-optimizer/README_zh.md)提供 `transfer-aware` provider，在预计完成时间上增加可配置的传输惩罚。可通过 `estimate` 进一步提供实测选择率或领域计算耗时。内置 `cost` 参数见用户指南，外部插件参数完全由 provider 定义。

```mermaid
flowchart LR
    S[不可变规划快照] --> P[插件估算与候选]
    P --> V[Union 约束验证]
    V --> C[核心关键路径估算]
    C --> R[插件评分]
    R --> B[有界候选选择]
```

## 安全与验证

Union 对所有候选（包括插件提供的分配）验证数据所有权、显式位置、容量及已知 executor／内存约束。插件不能增加成员、改变凭据、修改 runtime 画像、改变逻辑图或绕过发布策略。成员端再次验证环境兼容性。配置由管理员提供，不由 Job 提交者提供。

至少测试：已知任务选择预期成员；统计缺失时保守估算；非法分配／数值被拒绝；禁止来源／角色仍被拒绝；进程合并边界保持分离；取消向上传播；同一快照重复规划得到相同结果。核心契约测试见 `GraphPlacementSpec`，集成覆盖见 `UnionSpec`。

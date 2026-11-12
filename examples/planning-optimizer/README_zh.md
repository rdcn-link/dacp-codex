# 传输成本优先的规划插件

在预计完成时间上增加可配置的跨节点传输惩罚，所有节点分配仍由宿主校验。参见[扩展契约](../../docs/developers/planning-optimizer_zh.md)。

从本检出目录构建与宿主匹配的 SDK 和示例：

```bash
cd dacp-union-protocol
sbt publishLocal
cd ../examples/planning-optimizer
sbt package
mkdir -p /opt/dacp/union-plugins
cp target/scala-2.13/dacp-planning-example_2.13-0.1.0-SNAPSHOT.jar /opt/dacp/union-plugins/
```

JAR 包含 `META-INF/services/dacp.union.PhysicalPlanOptimizer`。配置 Union 后重启：

```yaml
plugin_directory: /opt/dacp/union-plugins
planning:
  provider: transfer-aware
  beam_width: 32
  settings:
    transfer_penalty_ms_per_mb: 10
```

惩罚值 10 表示每传输一个十进制 MB，增加 10 毫秒评分，表达策略偏好而不改变实测带宽。评分必须有限且非负。Job 不能安装此插件或覆盖其设置。用于生产前，应以代表性 Job 比较估算与实际传输量。

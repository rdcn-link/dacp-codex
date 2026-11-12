# Transfer-aware planning plugin

Adds a configurable cross-node transfer penalty to estimated completion time. The host still validates every placement. See [the extension contract](../../docs/developers/planning-optimizer.md).

Build the host-matching SDK and example from this checkout:

```bash
cd dacp-union-protocol
sbt publishLocal
cd ../examples/planning-optimizer
sbt package
mkdir -p /opt/dacp/union-plugins
cp target/scala-2.13/dacp-planning-example_2.13-0.1.0-SNAPSHOT.jar /opt/dacp/union-plugins/
```

The JAR contains `META-INF/services/dacp.union.PhysicalPlanOptimizer`. Configure the Union and restart it:

```yaml
plugin_directory: /opt/dacp/union-plugins
planning:
  provider: transfer-aware
  beam_width: 32
  settings:
    transfer_penalty_ms_per_mb: 10
```

A penalty of 10 means an additional 10 milliseconds per transferred decimal MB. It expresses policy preference; it does not change measured bandwidth. The score must remain finite and nonnegative. Jobs cannot install this plugin or override its settings. Test it on representative Jobs and compare estimates with observed transfers before production use.

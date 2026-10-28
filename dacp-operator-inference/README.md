# DACP operator structure inference

Shared structure inference for node servers and the online designer. The module resolves and downloads operators, starts and reuses inference workers on demand, reclaims idle workers, and propagates structures through directed stream graphs. Inference calls operator inference entry points without running workflows or reading data rows.

The host supplies `OperatorEnvironment` or creates `OperatorInferenceService` with a local operator library, remote repository URLs, cache path, size limit, `PluginEnvironment`, and worker settings. Closing the service reclaims workers; the host owns the plugin environment. Executors use existing plugin discovery without changes to core registries. The host supplies source metadata; the module does not depend on a particular dataset backend.

Validation: run `sbt test` in this directory. Graph tests cover missing ports, cycles, propagation, and errors; worker-pool tests cover reuse, concurrency, and reclamation. Docker integration tests require Docker access, and container startup time depends on host storage performance.

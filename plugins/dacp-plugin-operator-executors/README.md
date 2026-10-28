# DACP operator executors

[中文](README_zh.md)

Launch plugins for `container`, `jvm`, `python`, and `command` operators. All run inside a shared Docker runtime environment. JARs and Python packages are downloaded separately from that environment and mounted read-only; operator code is never loaded into the server JVM.

Built-in implementations register through `META-INF/services/dacp.OperatorExecutor`, the same contract used by external plugins. The core owns container isolation, timeout, cancellation, and cleanup. Plugins validate their own artifact and entrypoint and supply the launch command.

```bash
(cd plugins/dacp-plugin-operator-executors && sbt test)
```

See [operator packaging](../../docs/operator-packaging.md) for runtime layouts, definitions, publication, plugin registration, limitations, and contract tests.

The `command` plugin connects one bytes input to stdin and wraps stdout as a bytes output. It supports runtime-installed executables and separately published native artifacts. See [native command configuration, registration and tests](../../docs/native-commands.md).

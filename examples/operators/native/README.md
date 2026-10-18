# Native command operator example

This C program reads stdin and writes uppercase bytes to stdout. It has no DACP or Arrow dependency. The `command` executor bridges those streams inside the container.

From the project root:

```bash
mkdir -p examples/operators/native/build
cc -O2 -Wall -Wextra -Werror -s \
  -o examples/operators/native/build/uppercase examples/operators/native/uppercase.c
```

Build/install the shared `dacp/operator-python:1.0` runtime as described in the [operator guide](../../../docs/operator-authoring_en.md). It supplies the bridge's Python and PyArrow dependencies; the executable must match its Linux architecture and available C library. Static linking is optional.

Set the execution server's `local_operator_library` to the absolute path of `examples/operators/native/operators`, or seed a public repository:

```bash
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed \
  --operators examples/operators/native/operators --root .operator-catalog
```

Configure execution nodes with `remote_operator_library_urls` for repository downloads. In the connected shell:

```text
run native_uppercase@1.0 --input source=/reports/orders
```

The binary is downloaded separately and shares the Python runtime image. No execution permissions need to survive publication: the adapter makes an executable private copy. Outputs are streams; nothing is registered as a result dataset.

For a program already installed in a runtime, omit `artifact` and set `entrypoint` to its absolute runtime path. `executor_config.args` supplies literal arguments. See [native command configuration and publication](../../../docs/native-commands_en.md).

Validation uses the real server, mTLS and Docker, on temporary port 18817:

```bash
(cd dacp-server && sbt -batch stage)
.venv/bin/python scripts/test-native-execution.py
```

Run with Docker group access (for example, wrap the last command in `sg docker -c '...'` if needed). The test compiles the program and verifies output larger than pipe buffers, partial failure, incremental delivery, cancellation and recovery. It cleans up its server and temporary files. Native source submission, records conversion, and multiple input/output pipes are not implemented.

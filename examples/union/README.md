# Two-node weather collaboration

[中文](README_zh.md)

Node A publishes synthetic observations; node B publishes station climate baselines. Union coordinates streaming daily statistics and anomaly calculation. The client connects only to Union, using its public dataset namespace.

## Prepare

Build/stage `dacp-server` and `dacp-union-server` with `sbt test stage` in each module. Build the Python operator runtime as described in [Python operators](../operators/python/README.md). From the repository root:

```bash
bash scripts/certs.sh
.venv/bin/python examples/union/generate_data.py
.venv/bin/python examples/union/prepare.py
```

The preparation script creates the stable Union identity, node-issued service credentials bound to the Union certificate, and a user token. Secrets are written to protected files, not printed. Re-running preparation creates additional credentials; revoke obsolete ones with the local credential CLI.

## Start

Run these commands in separate terminals with the indicated working directories:

```bash
# dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-a.yaml
# dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-b.yaml
# dacp-union-server
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
# Repository root
PYTHONPATH=dacp-client .venv/bin/python examples/union/client.py
```

The client reads the online catalog, resolves public dataset names and passes the Item URIs as workflow variables. Source inputs are empty; processing inputs reference only upstream output streams. Operators must implement structure inference. Final outputs stream through Union; intermediate outputs stream between nodes. There is no explicit materialization policy in this example.

Open the internal read-only dashboard at `http://localhost:19880/`. Use Ctrl-C or SIGTERM to stop services. State and execution history persist; running jobs do not resume after restart. See [security](../../docs/security.md), [workflow variables](../../docs/jobs.md), [policy and replay](../../docs/materialization.md), and [Union configuration](../../dacp-union-server/README.md).

# Two-node Union collaboration example

[中文](README.md)

A aggregates local sales and outputs `{"apple": 6, "pear": 3}`. B pulls that stream directly, combines it with its own price table, and produces `{"apple": "9.00", "pear": "6.00"}`. The client connects only to X. Both nodes have empty ordinary download prefixes; Union receives compute permission only.

## Preparation

Run preparation from the repository root. Requires JDK 17, sbt, OpenSSL, Python, PyArrow, and access to the Docker daemon. With `.venv`, replace `python3` with `.venv/bin/python`. Nodes must be able to run the shared Python image; domain logic lives only in small Python source operators.

```bash
bash scripts/certs.sh
python3 examples/union/prepare.py
python3 -m pip install -e ./dacp-client
docker build -t dacp/operator-python:1.0 examples/operators/python
```

Preparation uses the existing `server` certificate as X and `client` certificate as C. It creates `.union-demo/x.yaml` and two approval policies valid for 24 hours. Example certificates are for local demonstration only.

Build the node and X:

```bash
cd dacp-server
sbt stage
cd ../dacp-union-server
sbt stage
```

## Start and invite

Use three terminals, retaining the indicated working directories:

```bash
# Terminal A, working directory dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-a.yaml
```

```bash
# Terminal B, working directory dacp-server
target/universal/stage/bin/dacp-server ../examples/union/node-b.yaml
```

```bash
# Terminal X, working directory dacp-union-server
target/universal/stage/bin/dacp-union-server serve ../.union-demo/x.yaml
```

In `dacp-union-server`, invite both nodes and inspect the Union ID:

```bash
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18861
target/universal/stage/bin/dacp-union-server invite ../.union-demo/x.yaml dacp://localhost:18862
target/universal/stage/bin/dacp-union-server identity ../.union-demo/x.yaml
```

From `dacp-server`, replace `UNION_ID` with the returned `union_id`. Each node's own administrator approves:

```bash
target/universal/stage/bin/dacp-server union approve ../.union-demo/a UNION_ID ../.union-demo/a-policy.yaml
target/universal/stage/bin/dacp-server union approve ../.union-demo/b UNION_ID ../.union-demo/b-policy.yaml
```

This explicitly grants X compute permission; a certificate from the same CA is insufficient. Query from X's directory:

```bash
target/universal/stage/bin/dacp-union-server members ../.union-demo/x.yaml
```

## Submit and observe

Return to the repository root:

```bash
python3 examples/union/client.py
```

The client submits [job.yaml](job.yaml), incrementally prints Task states and port traffic events to standard error, and prints final JSON to standard output. B pulls the intermediate stream directly from A over Arrow Flight mTLS; X receives only B's final stream. Analysis operators live in `operators/`, requiring no core registry changes; each node's `local_operator_library` points there. Python operators share one runtime image and each source artifact is only a few KB. For clarity, these demo operators limit each input to 16 MiB and aggregate within the operator; transport itself remains bounded streaming.

Stop each service with Ctrl-C. Membership approval persists in `.union-demo`; outputs are not saved. Already approved members do not need repeated invitation and approval. After expiry, the node leaves, then X invites it and its administrator approves again. To restart the entire example from scratch, stop every service, remove `.union-demo`, and repeat preparation.

[Configuration, interfaces, and limits](../../dacp-union-server/README_en.md); [protocol and development contract](../../docs/union_en.md).

## Automated verification

After `sbt stage` in both modules and preparing certificates and the Python image, run `python3 scripts/test-union.py` from the repository root. It chooses temporary ports and state, starts real A, B, X, and Docker operators, checks results, direct-transfer counters, and events, and stops every service automatically. The same Docker access permission is required.

## Browse the operations dashboard

`prepare.py` configures a local HTTP dashboard for X. After starting X, open `http://127.0.0.1:19880/` to inspect A and B's approval, health, and capacity. Submit the example job and select its ID to view both tasks' dependencies, states, and port traffic. The page refreshes every 2 seconds without claiming the client's result stream. Task communication still uses Flight. See the [HTTP operations dashboard](../../dacp-union-server/README_en.md#http-operations-dashboard) for remote access and token configuration.

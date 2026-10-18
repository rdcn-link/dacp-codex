# Collaborative DACP servers

Only B owns the CSV source. A coordinates a two-task streaming chain: B converts the item to Parquet, then A relays its bytes with identity. Flight DoExchange carries task requests and output frames over mTLS. No result dataset or retained output URI is created.

From the project root, prepare the environment and run the integration:

```bash
python3 -m venv .venv
.venv/bin/pip install -e './dacp-client[test]'
bash scripts/certs.sh
scripts/build-operator-image.sh
bash scripts/test-collaboration.sh
# Also exercise remote repository downloads and verified caches:
DACP_TEST_REMOTE_REPOSITORY=1 bash scripts/test-collaboration.sh
```

Docker access is required. The runner stages the server, uses temporary ports 18815/18816 (repository 18820), verifies exact Parquet content, repeated runs, upstream failure recovery, and client-certificate enforcement, then stops its servers. Override `DACP_TEST_PORT_A`, `DACP_TEST_PORT_B`, or `DACP_TEST_REPOSITORY_PORT` as needed. Logs remain in the printed temporary directory. `DACP_PYTHON` overrides `.venv/bin/python`.

For manual execution, run these in separate terminals from `dacp-server`:

```bash
sbt 'run ../examples/multi-node/node-a.yaml'
sbt 'run ../examples/multi-node/node-b.yaml'
```

Manual configurations use ports 8815/8816. Submit from the project root:

```python
import os
from pathlib import Path
from dacp import Job

os.environ['DACP_CA'] = 'examples/certs/ca.crt'
os.environ['DACP_CERT'] = 'examples/certs/client.crt'
os.environ['DACP_KEY'] = 'examples/certs/client.key'
with Job.run('dacp://localhost:8815',
             Path('examples/multi-node/job.yaml').read_text(), timeout=180) as events:
    for event in events:
        if event['type'] == 'data':
            print(event['port'], len(event['data']))
```

Repeated runs reuse output port names safely. Closing the iterator early cancels the pipeline. A successful `complete` event follows all output ends; failures after data delivery mean the displayed output is partial. Slow consumers apply backpressure. Streams cannot be reopened.

Tasks can choose an explicit `node`; otherwise placement follows their input anchor. `task_peers` authorizes dispatch, `task_input_prefixes` authorizes worker source reads, and `allowed_uri_prefixes` controls public source access. There is no central registry. On separate machines, replace localhost with advertised origins, provide matching certificate SANs, and configure permitted peers and source prefixes.

Each operator runs in an isolated container. `identity@1.1` preserves streamed bytes, including Parquet footers; it shares the JVM runtime with `format_convert@1.1`. Output ports have at most one consumer, and multiple inputs must be consumed in lexical order. General scheduling, fan-out, retries, checkpoints, and replay are not implemented. See [the streaming contract](../../docs/operator-streams.md).

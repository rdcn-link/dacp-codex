# Two-node streaming example

[中文](README_zh.md)

[job.yaml](job.yaml) reads the records view of `dacp://localhost:8816/csv_data/orders` and sends it to `piflow_csv_save@1.0` on the coordinating node at port 8815. Results return as a CSV byte stream. Publish and approve that operator in your repository, configure both nodes to use it, and configure their collaboration credentials before running this example. Adjust the addresses and Item path to your deployment.

From the repository root, set `DACP_CA` and `DACP_TOKEN_FILE` for the coordinating node, then run:

```bash
PYTHONPATH=dacp-client .venv/bin/python examples/multi-node/check.py
```

The node configuration examples illustrate publication and transport settings; supply deployment-specific credential directories and repository URLs. The maintained service integration suites create isolated nodes and test operators themselves:

```bash
bash scripts/test-collaboration.sh
```

A successful completion follows all output streams. Closing the client stream cancels the job. Consult [security](../../docs/security.md), [job execution](../../docs/jobs.md), and [operator publication](../../docs/operator-submission.md) for deployment details. Operators are ordinary approved repository packages; the server contains no identity or format-conversion implementation.

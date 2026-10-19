# Small Python operators

[中文](README.md)

This example distributes `identity.py` separately from a reusable Python/PyArrow environment. Its source artifact is a few kilobytes; Python and PyArrow are downloaded once per runtime digest. The operator preserves input bytes using the standard Arrow IPC boundary, including FIFO EOS handling.

Run from the project root, with Docker access and a staged operator repository:

```bash
docker build -f examples/operators/python/Dockerfile -t dacp/operator-python:1.0 .
python3 scripts/package-operator-sources.py
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository seed --operators examples/operators/python --root .operator-catalog
```

Reference `operator: identity_python@1.0` in a task and configure its node's `remote_operator_library_urls` to reach the repository. Alternatively, set `local_operator_library: ../examples/operators/python` when starting the node from `dacp-server`. This optional example is not included in the default starter catalog. Building this environment needs network access to obtain the Python image and pinned PyArrow dependency; task execution uses no network.

The included `pyproject.toml` builds the same source as an installable `identity` module. With setuptools and wheel installed in the build environment, build and publish:

```bash
.venv/bin/python -m pip wheel --no-deps --no-build-isolation --wheel-dir examples/operators/python/dist ./examples/operators/python
dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository publish identity_wheel@1.0 --root .operator-catalog --image dacp/operator-python:1.0 --executor python --entrypoint identity --artifact examples/operators/python/dist/dacp_example_ops-1.0-py3-none-any.whl --ports examples/operators/python/identity_python/1.0/operator.yaml
```

Dependencies must already exist in the runtime. The executor installs the wheel offline with `--no-index --no-deps`, then invokes its module. Multiple operators may reference different modules in the same wheel. See [packaging and executor contracts](../../../docs/operator-packaging_en.md).

For complete source examples, wheel packaging, remote file transfer, administrator publication, and execution instructions, see [Write and upload custom operators](../../../docs/operator-authoring_en.md).

New source-bearing versions and optional license/SBOM publication are described in [source and review information](../../../docs/operator-provenance_en.md). Package sources before seeding.

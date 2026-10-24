# Union DACP proxy service

[中文](union-proxy.md)

Union provides a standard DACP service, Union management Flight APIs, and a separate HTTP operations dashboard. Python/Scala `Source`, `Job`, and the shell can connect to its public Flight address. Members remain independent; Union invites and polls them, proxies approved members' data, and dispatches computation.

## Public dataset names

Nodes continue publishing datasets by local name. Union maps each (member certificate identity, local dataset name) pair to a fixed-length public name such as `ds-k7m2-9wqr`. Its public URI is `dacp://union.example:19860/ds-k7m2-9wqr`, followed by an Item's logical path. `name` is that public name. Datasets have no separate `id` or display-name property anywhere in the system. Member-local names are used only for internal routing. Public catalogs omit member addresses and certificate identities. User descriptions and payloads are preserved; publishers control the information they contain.

Names use a persistent increasing counter, keyed reversible permutation and an unambiguous alphabet. Adjacent counters are not apparent from their strings. Encoding is not authorization. Logical node IDs use `nd-xxxx-xxxx` for clients requiring explicit placement; obtain them through Flight Action `dacp.nodes`.

`state_directory/dataset-routes/routes.json` stores the encoding key, next counter and current routes, without dataset metadata replicas. A writer lock allows one serving instance. Back up the entire Union state directory; do not restore an old backup over an dataset namespace still referenced externally. Losing the key or rolling back the counter can break name stability.

## Online catalogs and membership changes

Action `dacp.catalog` accepts an empty body and returns a `Directory` protobuf with `datasets`, `complete` and `unavailable_sources`. Each request reads and merges approved members' catalogs online; newly discovered datasets receive names automatically. Unreachable, unapproved or failing members produce an explicitly incomplete directory containing available entries. Clients must not interpret partial directories as complete. Standard `ListFlights` cannot carry completeness, so partial catalogs fail with `UNAVAILABLE`.

Temporary failures preserve routes. Only a successful complete member listing confirming a missing name removes that mapping. Explicit member removal also removes its routes. Deleted public names are never reused; a returning local name receives a new public name. Revocation stops proxy access, and failures or revocation never redirect an old public name to another dataset. Replacing a dataset in place under the same native name cannot be detected as a new identity under this naming contract.

## Clients

```text
dacp-shell dacp://union.example:19860
dacp-shell> datasets
dacp-shell> use ds-k7m2-9wqr
dacp-shell> ls /
dacp-shell> cat /observations records
```

Shell startup binds the server address; `datasets` accepts no address argument. A complete, single-entry directory is selected automatically. `use NAME` selects the exact published name, such as `use ds-k7m2-9wqr`; after `datasets`, `use 1` selects the corresponding row. The list shows row numbers, names, and JSON descriptions, without URIs. SDK `Source.datasets(...)` returns an iterable directory with completeness fields. Item reads, record scans and bytes streams use ordinary DACP interfaces. S3 defaults to anonymous read-only access, using public names as buckets and Item logical paths as keys. Disable it with `s3: {enabled: false}`. Incomplete bucket listings return 503 because S3 cannot express partial directory completeness.

## Ordinary jobs and placement

Submit ordinary Jobs through standard `Job.run` or shell `submit`, using only public Union input URIs:

```yaml
job: climate-summary
tasks:
- id: read_summarize_source
  operator: read_item@1.0
  inputs: {}
  params:
    uri: dacp://union.example:19860/ds-k7m2-9wqr/observations
    view: bytes
- id: summarize
  title: Summarize observations
  operator: identity@1.3
  inputs:
    source: read_summarize_source
outputs:
  result: summarize
```

The example operator must be installed or available from a repository and its exact version approved by the member. Ordinary tasks may omit `node`. Union filters locations by approval, operator version and capacity, preferring input or upstream locations. When all raw input sizes are known, it chooses the location minimizing transferred bytes; otherwise it follows input declaration order. A multi-input operator executes at one member and reads other members' inputs as Flight streams. Explicit placement uses `node: nd-xxxx-xxxx`; public tasks cannot specify internal addresses.

Execution policy JSON, output streams, failed-attempt notifications, cancellation, timeout, member-side materialization and replay use the existing engine. Results stream through Union to the client by default. Union participates as an upstream member through the ordinary worker contract, delegating computation to its own members. Inputs use temporary, single-consumer, certificate-bound streams without disk staging. Proxy paths detect cycles and enforce an eight-level limit; cancellation and remaining execution time propagate downstream.

## Management boundaries

`client_identities` permits ordinary submissions; `administrator_identities` permits Union management APIs and internal topology views. A CA-issued certificate alone cannot submit computation; public discovery needs no additional business grant. Member approval permits proxying already-public data by default; computation remains subject to the member's operator, input and resource grants.

`dacp.union.members`, `invite_member` (`{"url":"dacp://node:18861"}`), and `remove_member` (`{"identity":"certificate fingerprint"}`) are administrator interfaces. The specialized asynchronous Union submission API accepts explicit internal placements for administrators. Ordinary clients use only the public namespace; status and failure messages omit internal topology. Node approval remains a node administrator operation.

The operations dashboard is public and read-only, exposing real member addresses, scripts, execution history and per-member catalogs without login or tokens. Configure its listener for remote access:

```yaml
administrator_identities: ["<administrator certificate SHA-256 fingerprint>"]
web:
  host: 0.0.0.0
  port: 19880
s3:
  enabled: true
  host: 0.0.0.0
  port: 20860
```

Omitting `web` disables the dashboard; omitting `s3.port` uses the Flight port plus 2000.

## Development and validation

`DatasetCatalog.directory` adds completeness to the catalog contract, defaulting to a wrapper over plugin `list`. `ProxyCatalog` forwards metadata, scans and streams through the same `DatasetAccess` contract without database or format branches. Plugins retain their existing registration mechanism and require no core registry changes. The shared JSONL bytes view for records Items also applies to proxy access and S3.

```bash
(cd dacp-union-server && sbt test)
(cd dacp-server && sbt 'testOnly dacp.DatasetCatalogSpec dacp.DacpServerSpec dacp.S3ServiceSpec')
(cd dacp-client-scala && sbt test)
PYTHONPATH=dacp-client .venv/bin/python -m pytest dacp-client/tests
```

Contract tests cover name encoding/decoding and restart, complete/partial catalogs, public URIs, Item streams, S3, placement, raw cross-member inputs, worker composition, authorization and cycle detection.

To materialize outputs when Union acts as an upstream member, configure top-level `materialization_store` using the same storage plugin contract. Storage resides at that Union layer; its downstream members need not materialize:

```yaml
materialization_store:
  provider: file
  settings:
    directory: .union-state/materializations
```

# DACP node web dashboard

[中文](node-web.md)

Each dacp-server starts a public, read-only HTTP dashboard by default. It binds to `0.0.0.0` on the Flight port plus 1000: Flight `8815` uses HTTP `9815`, and Flight `18861` uses HTTP `19861`. Startup logs report the actual listener. Flight ports above 64535 require an explicit HTTP port.

```yaml
web:
  enabled: true
  host: 0.0.0.0
  port: 19861
```

Disable the dashboard:

```yaml
web:
  enabled: false
```

`enabled` must be a boolean and `port` an integer from 0 to 65535. Port 0 requests an ephemeral port, available to embedded applications through `server.webPort`. An HTTP bind failure fails node startup and releases the Flight port and resources already acquired. Closing the node also closes HTTP.

The homepage displays the Flight address, startup time, uptime, task capacity, available executors, dataset directory, and paginated execution history. Select a record to view a G6 DAG with horizontal layout, server grouping, step titles, states, observed traffic, click details, zoom, and pan. Node and Union dashboards reuse the same renderer, styles, and offline G6 resources; both use the project DACP logo.

Union subjobs display the definitions and steps assigned to this node. External inputs are marked as cross-node inputs; dependencies between local steps retain their edges. Use the Union dashboard for the global workflow and scheduling state. Ordinary Jobs retain their scripts; the page displays dependencies from those definitions and reads step states from execution records. Unobserved traffic shows `—`. Running Union subjobs read current in-memory states and counters on demand; completed executions use persistent records. Historical records without task definitions do not receive invented DAGs.

Node status and the selected execution refresh every 2 seconds. Dataset directories are read on page load or manual refresh. The dashboard does not open Items, run operators, cancel jobs, or consume result streams.

The page publicly exposes execution metadata and stored scripts and task parameters. Use it where this information may be public, configure its bind address, or disable it. Flight mTLS, data authorization, and execution authorization retain their existing rules. Responses use `Cache-Control: no-store`; static resources require no external network access.

Union provides per-member dataset pages on its website, with no directory Flight API or aggregate of all members; see [dataset directories](dataset-directory_en.md).

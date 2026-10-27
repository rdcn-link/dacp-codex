# DACP node web dashboard

[中文](node-web.md)

Each dacp-server starts a public, read-only HTTP dashboard by default. It binds to `0.0.0.0` on the DACP service port plus 1000: DACP `8815` uses HTTP `9815`, and DACP `18861` uses HTTP `19861`. Startup logs report the actual listener. DACP service ports above 64535 require an explicit HTTP port.

```yaml
web:
  enabled: true
  host: 0.0.0.0
  public_host: node-a.example.org
  port: 19861
```

Disable the dashboard:

```yaml
web:
  enabled: false
```

`enabled` must be a boolean and `port` an integer from 0 to 65535. Port 0 requests an ephemeral port, available to embedded applications through `server.webPort`. An HTTP bind failure fails node startup and releases the DACP service port and resources already acquired. Closing the node also closes HTTP.

The homepage displays the DACP service address, startup time, uptime, task capacity, available executors, dataset directory, and paginated execution history. Select a record to view a G6 DAG with horizontal layout, server grouping, step titles, states, observed traffic, click details, zoom, and pan. Node and Union dashboards reuse the same renderer, styles, and offline G6 resources; both use the project DACP logo.

Union subjobs display the definitions and steps assigned to this node. External inputs are marked as cross-node inputs; dependencies between local steps retain their edges. Use the Union dashboard for the global workflow and scheduling state. Ordinary Jobs retain their scripts; the page displays dependencies from those definitions and reads step states from execution records. Unobserved traffic shows `—`. Running Union subjobs read current in-memory states and counters on demand; completed executions use persistent records. Historical records without task definitions do not receive invented DAGs.

Node status and the selected execution refresh every 2 seconds. Dataset directories are read on page load or manual refresh. The dashboard does not open Items, run operators, cancel jobs, or consume result streams.

The page publicly exposes execution metadata and stored scripts and task parameters. Use it where this information may be public, configure its bind address, or disable it. Flight uses TLS and tokens; published datasets are public by default, while execution retains its separate authorization rules. Responses use `Cache-Control: no-store`; static resources require no external network access.

Union provides public DACP proxy catalogs and per-member dataset pages on its management website; see [dataset directories](dataset-directory_en.md).

`web.public_host` is the browser-facing dashboard hostname and defaults to the node `public_host`; it is separate from the bind address `web.host`. Only after HTTP starts successfully does the node report `web_enabled: true` and `web_url`, including its actual port, through DACP `node_status`. The Union member table links to that address, shows “未启用” when Web is disabled, and an unknown state when node status is unavailable.

The dashboard interface uses English. Task titles, dataset names, JSON descriptions and user scripts retain their original content; dates and numbers use English locale formatting.

## Execution sources and internal routing

The job list shows only top-level jobs; child steps do not occupy separate rows. Expand Operator step records in job details to view each step’s operator, state, start time, duration and attempt number. Step records from all attempts are retained.

The Source column distinguishes `Direct client` submissions from `Union` assignments. Type shows `User job` or `Union sub-job`. Source filtering applies before pagination and resets to the first page when changed. Details show the Union ID, execution ID and parent-record association.

The dashboard HTTP endpoints `/api/history?source=client` and `?source=union` filter by origin. Omit `source`, or use `source=all`, for all top-level jobs; steps are excluded before pagination. `/api/executions/<record_id>` includes associated step records in `step_records`. History entries and details include `execution_source: client|union`; `kind` retains the precise record type. The server derives the source from execution records; it is not a user-supplied authorization field.

Execution topology shows local connections and cross-node boundaries graphically. Sub-job definition displays the complete persisted definitions dispatched to this node, preserving operators, parameters, inputs, materialization settings and remote `output_routes` without filtering or rewriting fields. Local inputs use `task:port`; remote inputs retain their location objects. There is no separate routing section. Ordinary jobs display Job script. A `consumer_identity` digest authorizes a remote output reader; it is not a data representation or delivery mode.

Mouse-wheel zoom is locked by default. Click 🔒 Mouse zoom to unlock it; click again to lock it. Toolbar zoom, Fit to view and 100% controls remain available.

While locked, scrolling the wheel over the DAG scrolls the page normally; only unlocked graphs capture wheel scrolling.

Cross-node input interfaces appear in a separate upstream-server group outside this node’s execution area. Interface cards identify the upstream step and output port, such as `parse.result`; they are not steps executed on this node. Dependency and swimlane layouts use the same ownership.

Jobs have a required readable `name` and a system-generated execution ID. Node and Union job lists share Name, Job ID, Source, Type, Status, Started at and Duration columns; details show the name and full ID. Job definitions, subjob definitions and execution policies display as YAML.

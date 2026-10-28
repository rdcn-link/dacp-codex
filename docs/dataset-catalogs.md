# Dynamic Dataset catalogs

[中文](dataset-catalogs_zh.md) · [Plugin architecture](plugins.md) · [Embedded server](embedded-server.md)

`DatasetCatalog` determines which Datasets a server currently publishes; `DatasetProvider` determines how one Dataset accesses its contents. Use catalog plugins when Dataset identities come from a database, business service, or application. Dynamic Items inside one Dataset remain the responsibility of its Dataset provider.

## Command-line deployment

The default `static` catalog builds Datasets from `datasets` configuration. Configure a dynamic catalog as follows; it cannot coexist with a nonempty `datasets` declaration:

```yaml
plugin_directory: ./.dacp-plugins
dataset_catalog:
  provider: example-jdbc-catalog
  source:
    jdbc_url: jdbc:postgresql://localhost:5432/metadata
    user_env: DACP_CATALOG_USER
    password_env: DACP_CATALOG_PASSWORD
```

These parameters belong to the independent `example.JdbcCatalogProvider` example. A complete node configuration also needs listener, TLS, and other standard settings. The plugin owns `source` parsing and validation; core has no JDBC-specific catalog branch. See [JdbcCatalogProvider.scala](../examples/plugins/src/main/scala/example/JdbcCatalogProvider.scala) and the [plugin example](../examples/plugins/README.md) for source, build, and installation. Install a JDBC driver and its dependencies alongside the plugin in `plugin_directory`, such as the PostgreSQL JDBC driver. Catalog database connections are independent of ordinary SQL data access configuration.

The example queries these registry tables. Your business system creates and maintains them; the plugin does not create tables automatically:

```sql
CREATE TABLE dacp_datasets (
  name VARCHAR(255) PRIMARY KEY
);
CREATE TABLE dacp_items (
  dataset_name VARCHAR(255) NOT NULL,
  item_path VARCHAR(1024) NOT NULL,
  file_path VARCHAR(4096) NOT NULL
);
INSERT INTO dacp_datasets VALUES ('reports');
INSERT INTO dacp_items VALUES ('reports', '/orders', '/srv/data/orders.csv');
```

Clients access `dacp://host:8815/reports`. `/orders` is public; `file_path` is private to the node. Every listing queries `dacp_datasets`; every open queries its `dacp_items` and creates an independent access snapshot. Registry additions and removals require no restart. JDBC uses bound parameters and a ten-second query timeout, closing connections after queries. Backend failures fail requests instead of masquerading as an empty catalog. Plugins needing caching, consistent transactions, or connection pooling implement those policies themselves.

## Contracts and registration

Public types reside in `dacp-plugin-api`:

```scala
trait DatasetCatalog extends AutoCloseable {
  def list(): Seq[DatasetInfo]
  def directory(check: () => Unit): DatasetDirectory = DatasetDirectory(list(check))
  def open(name: String): PublishedDataset
  // Overloads accepting check: () => Unit support cancellation checks.
}

trait DatasetCatalogProvider {
  def id: String
  def create(settings: Map[String, Object], context: CatalogContext): DatasetCatalog
}
```

`DatasetInfo` contains the public addressable `name`, JSON object description string `description`, with no separate ID or display-name property. `directory` defaults to wrapping `list`; aggregating catalogs may return `DatasetDirectory(entries, complete, unavailableSources)` and must identify partial results explicitly. `CatalogContext.createDataset(definition)` reuses installed Dataset providers; `FileDatasets.mixed` reuses file and SQL mappings. Native backends may implement `PublishedDataset` directly. Do not implicitly flatten scientific arrays, images, or groups into records.

Provide a public no-argument implementation constructor and include this file in the JAR:

```text
META-INF/services/dacp.DatasetCatalogProvider
```

Its contents are fully qualified implementation names, for example:

```text
example.JdbcCatalogProvider
```

Startup matches configured `provider` against the plugin's `id` and calls `create(source, context)` once. Missing or duplicate providers and invalid configuration fail startup. No core registry edits or server rebuilds are needed.

## Lifecycle, authorization, and discovery

- Catalogs must support concurrent requests. `list` returns a current snapshot with valid, unique node Dataset names. Backend discovery failures must throw.
- `open` throws `FileNotFoundException` for missing names. Its result is a caller-owned `PublishedDataset` lease. The server closes the request access handle and then its lease. Do not hand one shared closable object to multiple requests.
- Acquired leases must remain usable after registry removal or refresh. Underlying file or database removal can still fail a read; this does not guarantee content snapshots or persistent results.
- Override the `check` overloads and check cancellation inside query loops and expensive operations. Reading still follows existing backpressure and cancellation contracts.
- The server closes the catalog instance. Catalogs release only their own resources; application-injected shared pools remain application-owned.
- Directory information requires no additional data or Union execution grant and still uses Flight TLS / Token. Published datasets are readable by default; namespace and resource-path validation remain in place.

Discovery uses the Flight `dacp.catalog` Action, returning entries and completeness. Standard `ListFlights` returns a complete directory or fails for incomplete catalogs. Each `FlightInfo.app_metadata` is a `DataSet` protobuf containing public names, URIs and user-defined JSON object strings in `description`, without Item contents. See [dataset directories](dataset-directory.md) for descriptions, the shell and per-member browsing on the Union website.

List Datasets through the standard Python Flight interface from the project root:

```python
from pathlib import Path
import pyarrow.flight as flight

with flight.FlightClient(
    "grpc+tls://localhost:8815",
    tls_root_certs=Path("examples/certs/ca.crt").read_bytes(),
    cert_chain=Path("examples/certs/client.crt").read_bytes(),
    private_key=Path("examples/certs/client.key").read_bytes(),
) as client:
    for info in client.list_flights():
        print(info.descriptor.path[1].decode("utf-8"))
```

## Contract tests

Run `(cd dacp-server && sbt test)`. `DatasetCatalogSpec` loads the independent JDBC example through ServiceLoader and verifies registry additions/removals, reading, public access, backend failures, and lease retention through real Flight. It also checks invalid and duplicate identities, cancellation, initialization failures, and close counts. Plugin authors should cover equivalent cases along with their cache, transaction, and pool management policies.

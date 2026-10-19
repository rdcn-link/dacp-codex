# Dynamic Dataset catalogs

[中文](dataset-catalogs.md) · [Plugin architecture](plugins_en.md) · [Embedded server](embedded-server_en.md)

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

These parameters belong to the independent `example.JdbcCatalogProvider` example. A complete node configuration also needs listener, TLS, `allowed_uri_prefixes`, and other standard settings. The plugin owns `source` parsing and validation; core has no JDBC-specific catalog branch. See [JdbcCatalogProvider.scala](../examples/plugins/src/main/scala/example/JdbcCatalogProvider.scala) and the [plugin example](../examples/plugins/README_en.md) for source, build, and installation. Install a JDBC driver and its dependencies alongside the plugin in `plugin_directory`, such as the PostgreSQL JDBC driver. Catalog database connections are independent of ordinary SQL data access configuration.

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
  def open(name: String): PublishedDataset
  // Overloads accepting check: () => Unit support cancellation checks.
}

trait DatasetCatalogProvider {
  def id: String
  def create(settings: Map[String, Object], context: CatalogContext): DatasetCatalog
}
```

`DatasetInfo` contains only the public `name`. `CatalogContext.createDataset(definition)` reuses installed Dataset providers; `FileDatasets.mixed` reuses file and SQL mappings. Native backends may implement `PublishedDataset` directly. Do not implicitly flatten scientific arrays, images, or groups into records.

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
- Discovery grants no new permissions. URI authorization precedes opening a Dataset. Discovery lists only names authorized at Dataset scope. Clients with Item-only permission may still access known authorized Items without discovering the whole Dataset.

Discovery uses standard Flight `ListFlights`, currently with empty criteria only. Each `FlightInfo` has a `datasets/<name>` descriptor and existing `DataSet` protobuf `app_metadata` containing the public URI, without Item contents. Existing Python/Scala SDK access to known Dataset URIs is unchanged.

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

Run `(cd dacp-server && sbt test)`. `DatasetCatalogSpec` loads the independent JDBC example through ServiceLoader and verifies registry additions/removals, reading, authorization, backend failures, and lease retention through real Flight. It also checks invalid and duplicate identities, cancellation, initialization failures, and close counts. Plugin authors should cover equivalent cases along with their cache, transaction, and pool management policies.

# Embedded DACP Server example

[中文](README.md) · [Embedding API](../../docs/embedded-server_en.md) · [Catalog contract](../../docs/dataset-catalogs_en.md)

This application starts DACP Server in its own JVM and directly injects a `DatasetCatalog`, without ServiceLoader registration. After startup, it adds `reports` to a concurrent registry and publishes the `/orders` CSV Item. The application can continue adding or removing entries while running.

Run from the project root with port 8815 available:

```bash
bash scripts/certs.sh
(cd examples/embedded-server && sbt 'run ../certs ../../examples/data/nested/orders.csv')
```

In another terminal, connect from the project root:

```bash
export DACP_CA="$PWD/examples/certs/ca.crt"
export DACP_CERT="$PWD/examples/certs/client.crt"
export DACP_KEY="$PWD/examples/certs/client.key"
dacp-shell dacp://localhost:8815
# shell:
# datasets
# use reports
```

Use `ls /` and `cat /orders` in the shell. Stop the foreground example with Ctrl+C. The example application installs its own shutdown hook; the embedded API does not install one automatically.

Requirements are JDK 17, sbt 1.10.x, and the installed Python SDK/shell. This example only reads CSV and needs no Docker. Its source-project dependency is `../../dacp-server`; see [build.sbt](build.sbt) for Arrow JVM options and [EmbeddedServerExample.scala](src/main/scala/example/EmbeddedServerExample.scala) for the full source.

Each returned `PublishedDataset` is request-owned and closed by the server after access. Registry refresh does not actively close existing handles, but does not freeze underlying data. If the host uses a shared database pool, retain ownership and close it after the server stops. See [JdbcCatalogProvider.scala](../plugins/src/main/scala/example/JdbcCatalogProvider.scala) for an independent database catalog plugin.

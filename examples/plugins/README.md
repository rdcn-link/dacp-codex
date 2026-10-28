# Standalone contributor plugins

[中文](README_zh.md)

This project compiles against `dacp-plugin-api` only. It does not depend on the server implementation. It supplies two registered plugins:

- `example-message`: a native Dataset with `/message` and `/summary`, configured by `source.message`.
- `example-message-file`: a `.message` structured-file handler with direct `/message` and `/summary` children and optional private `options.prefix`. Child bytes are UTF-8 text including the configured prefix; file-root bytes are unchanged.

`/message` offers UTF-8 bytes. `/summary` offers one Arrow record with a `characters` column counting Unicode code points; its explicit record count is 1.

The small example reads its text in memory; production handlers should use bounded streaming for large content.

From the repository root:

```bash
(cd dacp-plugin-api && sbt publishLocal)
(cd examples/plugins && sbt package)
(cd dacp-server && sbt stage)
mkdir -p plugins
cp examples/plugins/target/scala-2.13/dacp-example-plugins_2.13-0.1.0-SNAPSHOT.jar plugins/
```

Add this fragment to a complete server configuration (including listener and TLS settings). Relative paths resolve against the server working directory; this example starts from `dacp-server/`:

```yaml
plugin_directory: ../.dacp-plugins
datasets:
  greeting:
    provider: example-message
    source:
      message: Hello from a contributed dataset
  files:
    provider: mixed
    items:
      /document:
        file: /private/example.message
        options: {prefix: "Preview: "}
```

Restart the staged server with that configuration. No launcher edits, server rebuild for the plugin, or client changes are needed. The `stage` command above builds the server initially; subsequent plugin changes only require rebuilding the plugin JAR and restarting the server.

```python
from dacp import Source

with Source.open("dacp://localhost:8815/files") as ds:
    print(ds.list("/document"))
    with ds.open("/document/message") as stream:
        with stream.as_bytes() as reader:
            print(reader.read().decode("utf-8"))
```

Registration files live in `src/main/resources/META-INF/services/`. File and structured-file handlers use `dacp.FormatHandler`; dataset providers use `dacp.DatasetProvider`. Archive plugins use `dacp.ContainerReader` and automatically receive a format entry. Plugin JARs and their private dependency JARs belong directly inside `plugin_directory`; nested directories are not scanned. Do not package another copy of the DACP API or server-supplied libraries.

The server's [PluginSpec](../../dacp-server/src/test/scala/dacp/PluginSpec.scala) compiles this source into its tests and exercises it over Flight, alongside registered archive plugins and lifecycle checks. Run `PluginChecks.listing(access)` in your own tests to check recursive listing, immediate children, paths, and metadata. Add format-specific streaming, bounded-memory, error, cancellation, and resource-release tests.

The dynamic catalog example `example.JdbcCatalogProvider` registers through `META-INF/services/dacp.DatasetCatalogProvider`, queries database registry tables, and constructs Datasets on demand. After building this project, install its JAR and the JDBC driver in the plugin directory. See the [DatasetCatalog guide](../../docs/dataset-catalogs.md) for configuration and table definitions.

Once published by a node, `/summary` also receives an automatic JSONL `bytes` view. With `message: hello`, its bytes are `{"characters":5}` followed by a newline; the plugin still implements only records. See the [common view rules](../../docs/dataset-access.md#common-jsonl-bytes-view).

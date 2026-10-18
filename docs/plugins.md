# Contributing plugins

DACP is intended to grow through independently contributed plugins. Adding a dataset backend, file format, or other implementation of an established extension point should require installing and configuring a plugin, without editing core registries, rebuilding the server, or changing clients.

The core owns the common contracts, plugin discovery and lifecycle, Flight transport, authorization, namespace validation, cancellation, and resource management. Plugins own domain behavior, private settings, and third-party backend or parsing dependencies. Built-in implementations should use the same contracts as contributed plugins.

## Available extension points

| Contribution | Current mechanism | Guide |
|---|---|---|
| Dataset provider | Implement `DatasetProvider`; register through Java `ServiceLoader` | [Dataset providers](dataset-providers.md) |
| File format handler | Implement `FormatHandler` or `StructuredFileHandler`; register through Java `ServiceLoader` | [Format handlers](format-handlers.md) |
| Archive reader | Implement `ContainerReader`; register through Java `ServiceLoader` | [Container readers](format-handlers.md#container-readers) |
| Processing operator | Supply a container image and its input/output contract; publish in the operator library | [Scala operator library](../dacp-operator-library/README_en.md) |

Dataset providers and format handlers currently run inside the JVM server. They must be installed by the server administrator in `plugin_directory` with their private dependency JARs, and loaded on restart. They are not sandboxed or uploaded by clients. Operators execute separately in containers.

Dataset providers construct logical namespaces and implement listing, metadata, counting, and streaming. File handlers interpret content. Operators perform transformations. A new format should be usable from both directory-backed and custom datasets without adding a new dataset provider just for that format.

## Contribution contract

Each plugin should provide:

- A unique identity, registration metadata, installation instructions, and a minimal configuration example.
- Validation of its own required settings, types, and unknown parameters. Physical paths, credentials, and backend identifiers remain private.
- Accurate schemas and advertised capabilities. Original-file bytes remain available without changing the source; record counts have a defined meaning and remain unknown when unavailable.
- Bounded streaming, demand and cancellation handling, and cleanup on success, failure, and interruption. Ordinary browsing must not trigger expensive record scans.
- Tests covering discovery, configuration, namespace behavior, streaming, counts, cancellation, cleanup, and public errors without private diagnostics, as applicable to the extension.

Published item paths and views are the client contract. Backend-specific parsing and configuration belong on the server. Introducing a genuinely new content representation may require a shared protocol and SDK change; installing another implementation of an existing representation should not.

## Plugin foundation

The standalone [dacp-plugin-api](../dacp-plugin-api/README.md) module contains the shared contracts, without a dependency on the server implementation. The [example project](../examples/plugins/README.md) demonstrates independent compilation, service registration, native Dataset construction, and direct structured-file child access. `PluginChecks.listing` provides reusable metadata contract checks; the server suite tests end-to-end Flight integration, cancellation, and cleanup.

Configure `plugin_directory` to load plugin and private dependency JARs before registries initialize. Built-ins and contributed implementations use the same interfaces. Dataset providers receive a `DatasetContext` rather than the server's `Config`. Each handler validates its private YAML `options` map; `format` and `schema` remain common reader settings. Access handles are closed per request, and provider resources are closed on shutdown. Plugins are trusted server code and are not hot-reloaded.

Structured files implement `StructuredFileHandler.openStructure` to return logical children and stream them directly, without extraction. Existing ZIP/TAR/GZIP readers use the separately pluggable `ContainerReader` for staged entries. A container registration automatically supplies its file-format entry.

## Scientific data and future extensions

NetCDF and FITS parsers are not implemented yet. Contributors can use the structured-file contract for groups, variables, tables, and HDUs, with handler-owned selection parameters. Reuse `folder` for browsable structures and preserve original-file bytes at the file root. Child naming and byte encodings must be documented and deterministic.

Current public views remain `records` and `bytes`. New array/image views still require explicit shared schemas, metadata, and selection semantics. Do not silently flatten scientific arrays or invent record counts. Extending the plugin foundation does not imply those representations already exist.

New extension points should follow the same principle: define a focused shared contract, discover implementations through registration, and keep implementation-specific branches out of core dispatch. Do not introduce an extension abstraction without a concrete contribution or use case. During unreleased development, improve contracts directly and update their users together rather than accumulating compatibility layers.

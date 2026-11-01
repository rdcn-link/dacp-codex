# Use plugins in DACP

The standard distribution bundles its data readers and execution plugins. Install additional plugins into the server configuration’s `plugin_directory`, then restart.

- [Server plugin installation and lifecycle](../docs/plugins.md)
- [DACP integration example](../examples/plugins/README.md)
- [XStream plugin development and registration](https://github.com/rdcn-link/xstream-codex/blob/main/docs/plugins.md)
- [XStream data implementations and dependencies](https://github.com/rdcn-link/xstream-codex/blob/main/xstream-data/README.md)

This source directory contains integration documentation; use a separate path such as `.dacp-plugins/` for local plugin installation.

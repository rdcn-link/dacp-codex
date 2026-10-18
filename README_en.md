# DACP

<img src="assets/dacp-logo.png" alt="DACP logo: connected nodes forming a C for Collaborative" width="480">

[中文](README.md)

DACP is a data collaboration protocol and prototype built on Apache Arrow Flight. Nodes publish data through a common interface: clients browse files, SQL tables, and archives and compressed files using logical names, and submit YAML tasks to run computation where the data resides.

The project includes a Scala 2.13 server, Python and Scala client SDKs, and the interactive `dacp-shell` terminal. Nodes are addressed directly by URI and connect through mTLS, without a central registry.

## Documentation

| Audience | Start here | Topics |
|---|---|---|
| Users and node administrators | [User guide](docs/users/README_en.md) | Quick start, shell examples, SDKs, dataset setup, queries and streaming jobs |
| Operator authors and project contributors | [Developer guide](docs/developers/README_en.md) | Architecture, operator authoring, plugin contracts, protocol and tests |

[Documentation index](docs/README_en.md) · [中文文档](docs/README.md)

For a local example, follow the [quick start](docs/users/README_en.md#quick-start). In dacp-shell, type `help` for example usages or `help run` for operator execution examples.

The server uses JDK 17 and Scala 2.13; clients include Python, Scala and dacp-shell. Browsing does not require Docker. Operator execution uses isolated Docker containers and returns streams, without saving result datasets. The project is unreleased and does not retain superseded interfaces for compatibility.

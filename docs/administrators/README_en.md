# Administrator guide

[中文](README.md) · [Documentation](../README_en.md)

Administrators deploy data nodes, Union Server, and the operator repository, and maintain certificates, authorization, published data, and execution records. See the [DACP protocol](../protocol_en.md) for protocol and identity boundaries.

For Dataset identities supplied by a database or application, use a [dynamic catalog plugin](../dataset-catalogs_en.md). Applications hosting a node directly can use the [embedding API](../embedded-server_en.md).

## Single-node deployment

Servers require JDK 17 and sbt 1.10.x. Operator execution also requires Linux and access to Docker Engine. Clients browsing data do not require Docker. Run `docker info` to check access for the service account; group membership changes require a new login.

Build from the project root:

```bash
(cd dacp-server && sbt stage)
(cd dacp-union-server && sbt stage)
(cd dacp-operator-repository && sbt stage)
```

Local demonstrations use the project's certificate script and example configuration. Production deployments need their own CA, hostnames, and certificate management. Relative configuration paths resolve against the process working directory.

```bash
bash scripts/certs.sh
cd dacp-server
sbt 'run ../examples/server.yaml'
```

## Publishing data and operators

Declare Dataset providers and their parameters in node configuration. Built-in and external providers share the same plugin contracts. See the [technical reference](../reference_en.md), [Dataset providers](../dataset-providers_en.md), and [SQL example](../../examples/sql/README_en.md) for directory and SQL configuration. Restrict data directories and credentials to the service account.

Nodes read local operators through `local_operator_library` and search remote repositories through `remote_operator_library_urls`. Remote configuration order affects lookup order. Publish shared runtimes separately from operator code. See [packaging](../operator-packaging_en.md) and the [executor manual](../../plugins/dacp-plugin-operator-executors/README_en.md) for execution environments and native commands. Source submission is available only in DEBUG mode; see [source runtimes](../operator-source_en.md).

## Certificates and authorization

Flight uses mTLS: callers verify servers and servers verify clients. Nodes making cross-node calls also act as clients. CA signatures do not grant business permissions. Configure node data permissions, Union client authorization, and node approval of Union computation separately. Certificates must match connection hostnames and TLS purposes. Do not commit private keys or use the demonstration CA in production.

## Union and repository

- [Union manual](../../dacp-union-server/README_en.md): configuration, invitations, approval, polling, job submission, and public dashboard.
- [Two-node example](../../examples/union/README_en.md): complete certificate, node, Union, and client demonstration.
- [Repository manual](../../dacp-operator-repository/README_en.md): initialization, publication, source materials, HTTP service, and downloads.

Union maintains one Union, inviting independent nodes and polling their status. Joining requires node approval. Its HTTP dashboard is publicly readable and exposes job scripts and execution metadata. Review task parameters for information unsuitable for public viewing. Computation remains protected by authorization on Flight interfaces.

## Monitoring, records, and shutdown

The dashboard shows node health, start time, uptime, job DAGs, status, and traffic. Unreachable node status may be unknown. Nodes and Union persist execution metadata, rather than historical result files. See [execution history](../execution-history_en.md) for storage, queries, and restart behavior. Back up persistent configuration, membership approvals, and execution records. Stop services before copying to avoid inconsistency across files.

Use Ctrl+C for foreground processes or send SIGTERM. Restarting does not resume interrupted result streams. Inspect operator images with `docker image ls`; local runtime configuration and repository artifacts are separate resources. DEBUG logging and the DEBUG execution mode that allows source submission are configured separately.

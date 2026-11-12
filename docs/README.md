# DACP documentation

Use the User guide for connections, data access and Jobs; the Operations guide for deployment, publication, security, monitoring and maintenance; and the Development guide for application integration, embedded services and protocols.

[中文](README_zh.md)

## User guide · Get started

- [User guide](usage.md)
- [Frequently asked questions](faq.md)
- [First connection](quickstart.md)
- [Shell and SDK examples](users/README.md)

## User guide · Remote data access

- [Dataset discovery](dataset-directory.md)
- [Remote record queries](records-scans.md)
- [S3-compatible access](s3-service.md)
- [Public-WAN CSV filtering measurements](csv-filter-benchmark.md)
- [Scala client SDK](../dacp-client-scala/README.md)

## User guide · Jobs and results

- [Remote Job submission](jobs.md)
- [Physical plan generation and optimization](physical-planning.md)
- [Execution policies and replay](materialization.md)
- [Dashboards and plans](node-web.md)
- [Check-dam collaboration versus FTP download-first analysis](check-dams-benchmark.md)
- [Local and cross-node measurements](local-vs-union-benchmark.md)
- [Streaming and materialization measurements](materialization-benchmark.md)
- [Union public catalog and Jobs](union-proxy.md)

## Operations guide · Deployment and security

- [Operations guide](operations/README.md)
- [Credentials and roles](security.md)
- [Union deployment](../dacp-union-server/README.md)
- [Weather collaboration example](../examples/union/README.md)
- [Multi-node example](../examples/multi-node/README.md)
- [Publish SQL tables](../examples/sql/README.md)
- [Proxy deployment](../dacp-proxy-server/README.md)
- [Install server plugins](plugins.md)
- [Service configuration reference](reference.md)

## Operations guide · Monitoring and maintenance

- [Monitoring and dashboards](operations/monitoring.md)
- [Access logs](access-logs.md)
- [Execution-history maintenance](execution-history.md)

## Development guide · Service integration

- [Development guide](developers/README.md)
- [Embed a server](embedded-server.md)
- [Embedded application example](../examples/embedded-server/README.md)
- [Plugin deployment example](../examples/plugins/README.md)
- [Python collaboration SDK](../dacp-collaboration-client/README.md)
- [Scala collaboration SDK](../dacp-collaboration-client-scala/README.md)

## Development guide · Protocol and reference

- [DACP protocol](protocol.md)
- [Union collaboration protocol](union.md)
- [Union transport API](../dacp-union-protocol/README.md)
- [DEBUG source execution](operator-source.md)

For data models, formats, Job authoring and operator development, use the [XStream documentation](http://xstream.conoai.cn/docs.html).

[Dataset publication policies](dataset-publication.md): public, private and timed publication, including Union trust boundaries.

- [Extend planning optimization](developers/planning-optimizer.md)

# DACP and XStream project responsibilities

[中文](ecosystem_zh.md)

## Powered by XStream

DACP Server embeds the XStream streaming execution engine and shares its JVM DataCatalog. DataCatalog → Dataset → Item exposes typed data views; Job steps connect through streams. The engine provides local execution, cancellation, execution policies, optional materialization/replay and eligible same-process operator fusion. XStream also provides a standalone Runner, so the engine is reusable outside a DACP server.

DACP adds authenticated data publication and access, client SDKs and Shell, independent S3 access, execution history and service dashboards. Union provides member management, health polling, automatic placement, subjob dispatch and direct member-to-member stream routing. A client submits a logical Job to a service without needing to know whether it is a node or Union.

## Repositories and responsibilities

| Project | Owns | How DACP uses it |
|---|---|---|
| [dacp-codex](https://github.com/rdcn-link/dacp-codex) | Data services, Union, proxy, clients, public integration contracts and service plugins | Main service project |
| [xstream-codex](https://github.com/rdcn-link/xstream-codex) | Data SDK and format plugins, engine/Runner, Designer, operator SDK, inference, executors and source-analysis plugins | Versioned library dependencies and independent design tools |
| [xstream-operator-repository](https://github.com/rdcn-link/xstream-operator-repository) | Public operator/runtime catalog, source-only submission, isolated build and administrator review | Configured public package source; independently deployed |
| [dacp-website](https://github.com/rdcn-link/dacp-website) | DACP overview, service documentation and downloads | Independent official website |
| [xstream-website](https://github.com/rdcn-link/xstream-website) | Engine/Designer documentation, operator development/deployment guides, repository directory and integrated Designer demo | Independent ecosystem documentation and design experience |

## Build and deployment

Ordinary DACP builds resolve versioned XStream artifacts rather than adjacent source directories. Unreleased development artifacts need the explicit bootstrap documented in the XStream repository. Repository service builds resolve the operator contract and source-analysis plugin as versioned artifacts; no DACP service is required by that service. Websites carry their own static resources and documentation. The XStream website's optional integrated demo additionally deploys its own Designer distribution and inference environment.

Deploy DACP data nodes and Union independently from the repository, Designer and websites. Configure `remote_operator_library_urls` on execution services. Source submission to a repository is authenticated and reviewed; it is separate from DEBUG-only source execution on a data node. Approved operator packages are public. These boundaries do not remove API or artifact dependencies between cooperating projects.

## Where to read

Use DACP documentation for data access, service authorization, collaboration, Job submission and operations. Use XStream documentation for engine embedding, typed streams, Designer, operator interfaces and deployment. Use the repository website for catalog downloads, environment records and administrator review. Website builds include readable on-site documentation; source-repository links remain useful for implementation and contribution.

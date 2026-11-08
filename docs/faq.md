# Frequently asked questions

[中文](faq_zh.md)

## 1. How does a node join a Union?

The Union connects to a configured node using a service credential issued by that node. Nodes do not register themselves or send invitations.

1. Prepare the Union configuration and query its stable identity on the Union host:

   ```bash
   union-admin identity /etc/dacp/union.yaml
   ```

   Record `union_id` and `scheduler_identity`. Keep the Union state directory and certificate stable.

2. On the node host, issue a credential bound to that Union identity. Use the node's configured `credential_directory`:

   ```bash
   dacp-admin grant-union /protected/node-credentials \
     --subject union-service \
     --union-id UNION_ID \
     --fingerprint SCHEDULER_IDENTITY
   ```

   Replace the uppercase placeholders with the identity values from step 1. Save only the returned `token` value, not the complete JSON, in a protected text file. Transfer it securely to the Union host and restrict file permissions, for example with `chmod 600`.

3. Add the node to the Union configuration:

   ```yaml
   members:
     - url: dacp://node.example.org:18861
       credential_file: /protected/union/node.token
   ```

   Ensure the services can connect and their TLS configuration permits the connection. The node must have Union execution enabled; see the deployment guide for its `union` configuration.

4. Restart the Union to apply its member list, then diagnose the connection:

   ```bash
   union-admin members /etc/dacp/union.yaml
   ```

   The Union dashboard also shows member health. An unavailable member does not prevent Union startup; the service continues polling it.

See [Union deployment](../dacp-union-server/README.md) and [credentials and roles](security.md).

## 2. Can one node belong to multiple Unions?

Yes. Issue a separate service credential for each Union, bound to its Union ID and certificate identity. Add the same node to each Union's member configuration, using the corresponding credential. Each Union manages its own public dataset names and Jobs. Revoking one Union's credential does not revoke another's.

The node shares its execution capacity across local Jobs and all Unions. There are no separate per-Union resource reservations or fairness guarantees. A node's membership in two Unions does not make the other members of those Unions automatically trust each other. Accepted Union credentials authorize the Union's service operations; there is no per-Union dataset allowlist.

## 3. Which permissions and roles are available?

| Role | Permissions |
|---|---|
| `reader` | Public content and local presigning-tool access |
| `runner` | Reader capabilities, Job execution, inference and management of owned Jobs |

Dataset discovery, Item listings and Item metadata are public. Content follows dataset publication policies: public, private or embargoed. Public contents are anonymously readable; ordinary roles do not override private or active embargoes. Job execution requires runner permission. Union service credentials use a separate certificate-bound trust mechanism. Administrators assign the predefined roles to accounts; individual log operation names are not configurable role permissions. See [security](security.md).

## 4. Do steps and cross-node Jobs have time limits?

There is no separate inter-step idle timeout. A downstream step can wait for upstream data, and upstream steps wait when downstream buffers are full. These waits consume execution time.

The default execution limit is 1,800 seconds. Configure `execution_timeout_seconds` on the service; a submitted execution policy can shorten it with `timeout_seconds`. Union passes the remaining allowance to sub-jobs, which are also limited by the node's Union execution settings. Control requests usually have a separate 30-second deadline; data stream calls also have deadlines. Continuous data arrival does not extend a call's deadline. See [execution policies](materialization.md).

## 5. Does a 30-second deadline error mean the Job ran for too long?

Not necessarily. `CallOptions deadline exceeded after 29.99…s` indicates a particular remote call exceeded its deadline, rather than the overall Job reaching its execution limit. Check the business operation, request ID and Job/execution ID in the Union and node runtime logs. Increasing `execution_timeout_seconds` alone does not change the control-call deadline.

The node acknowledges an accepted start request before resolving operators and preparing execution units in the background. Acceptance does not mean initialization succeeded. Inspect subsequent Job status. An already cancelled, expired or completed sub-job cannot start again. “No step snapshots were recorded” means the dashboard has no step observations; it does not prove the node never started work.

## 6. Where are request logs and complete exception stacks?

Node access logs use JSON Lines in `access_log.directory`, or `.dacp-access/<port>/` relative to the working directory by default. They record completed requests asynchronously, including request ID, available Job ID, operation, status and duration. A start timestamp is not an independently emitted request-arrival record.

Operations use business names such as `catalog.list`, `item.describe`, `item.read`, `job.submit`, `job.prepare`, `job.start`, `job.status` and `job.cancel`. These differ from authorization permissions such as `data:read`. The separate transport authentication audit still records RPC methods.

Server runtime logs contain full exception stacks and cause chains with correlation identifiers. Before a Job exists, its Job ID is unavailable. Clients receive concise errors; access logs do not contain stacks. See [access logs](access-logs.md).

## 7. Do dashboards refresh automatically?

Node and Union dashboards load when opened and update on manual **Refresh** or **Reload**. Changing a page, filter or expanded dataset can load the requested information. The service's background member polling continues independently of browser refresh. The CONET large-screen display retains its automatic updates. See [dashboards](node-web.md).

## 8. How do I disable the dashboard Web service?

**DACP node:** set the following in the node startup configuration and restart the node:

```yaml
web:
  enabled: false
```

The node dashboard is enabled by default, so removing the node's `web` section does not disable it.

**Union Server:** remove the entire top-level `web` section from its startup configuration, then restart the Union. For example, remove this section:

```yaml
web:
  host: 127.0.0.1
  port: 19880
```

Union starts its dashboard only when that section is present. Its configuration does not accept `web.enabled: false`, `web: false`, or `web: null`.

Disabling the dashboard closes its HTTP listener and read-only HTTP endpoints. It does not disable the DACP data service, Job execution, Union coordination, execution history, or a separately configured S3 service. The node/Union process must be restarted to apply the change. See [dashboards](node-web.md) and [Union deployment](../dacp-union-server/README.md).

## 9. What operations does DACP support, and what are their business names and permissions?

| Operation | Business operation name | Access requirements |
| --- | --- | --- |
| Query service capabilities | `service.capabilities` | Anonymous |
| List datasets | `catalog.list` | Anonymous |
| Read catalog snapshot | `catalog.describe` | Anonymous |
| List Items / expand Folders | `item.list` | Anonymous |
| Describe Item metadata | `item.describe` | Anonymous |
| Count records | `item.count` | Dataset content publication policy |
| Read an Item View | `item.read` | Dataset content publication policy |
| Infer step outputs | `step.infer` | `job:execute` |
| Infer Job outputs | `job.infer` | `job:execute` |
| Submit a Job | `job.submit` | `job:execute` |
| Replay a Job | `job.replay` | `job:execute` |
| Query Job status | `job.status` | `job:manage:self` and ownership |
| Cancel a Job | `job.cancel` | `job:manage:self` and ownership |
| Read Job results | `job.result.read` | `job:manage:self` and ownership |
| Read Job events | `job.events.read` | `job:manage:self` and ownership |
| List available nodes | `node.list` | `job:execute` |
| Query node status | `node.status` | Trusted service identity and execution-plan authorization |
| Query Union membership | `union.membership.get` | Trusted service identity and execution-plan authorization |
| Prepare a sub-job | `job.prepare` | Trusted service identity and execution-plan authorization |
| Start a sub-job | `job.start` | Trusted service identity and execution-plan authorization |
| Read step output | `step.output.read` | Trusted service identity and execution-plan authorization |
| Query step materialization status | `step.materialization.status` | Trusted service identity and execution-plan authorization |

Catalogs, listings and metadata are always public. Item content reads and explicit scans follow the dataset publication policy; Job submission privileges do not bypass it. S3 listings and object metadata are anonymous; GET content follows the same dataset policy. Supplied signatures must still be valid.

Filtering, projection, limits and scientific slicing are `item.read` parameters. Business names identify logs, not remote methods or permission identifiers. Dashboards are read-only; configure listeners and firewalls for internal access. Local administration uses `dacp-admin` and `union-admin`. See [access logs](access-logs.md), [authentication and access control](security.md) and [dataset publication policies](dataset-publication.md).

## 10. What is `/etc/dacp/ca.crt`?

It is an example path to a public CA certificate used to validate the remote service's TLS certificate. Use the actual CA certificate path for your deployment. The certificate is not an access token or a private key, and it grants no data-read or Job-execution permissions.

All connections verify the server certificate and hostname. Omit an explicit CA only if the platform trusts the server certificate; configure `ca` for private or self-signed CAs. In Designer connection settings, this path is on the Designer server, not the visitor's computer. Account tokens are configured separately through `token_file`. See [authentication and access control](security.md).

## 11. How can I restrict DACP services to an internal network?

Configure the DACP, dashboard and S3 listeners separately. For a host with private address `10.0.82.103`:

```yaml
host: 10.0.82.103
port: 19860
public_host: 10.0.82.103
web:
  host: 10.0.82.103
  port: 19880
s3:
  enabled: true
  host: 10.0.82.103
  port: 21860
```

This is DACP Server configuration. Union declares its advertised address with `public_url: dacp://10.0.82.103:19860`, rather than `public_host`. For host-only access, bind each listener to `127.0.0.1`; access through the private IP then does not work. Disable unused dashboards and S3 services; dashboard shutdown is described elsewhere on this page.

Binding a private address limits listening interfaces, but does not check request sources. Also restrict the actual DACP, dashboard and S3 ports to approved private source subnets using host firewalls, cloud security groups and network boundary rules. Prevent public port forwarding and proxy exposure, and check IPv6 ingress rules. Restart after configuration changes and verify reachability from both permitted and prohibited networks. `public_host` / `public_url` advertise addresses; they are not access controls. Network restrictions do not replace TLS server identity verification or operation authorization.

# DACP proxy server

[中文](README_zh.md)

A Scala single-upstream TCP relay. Its host must reach the private DACP service through routing or a VPN. Each instance proxies exactly one service.

```text
Client → public proxy:28815 → private DACP:8815
```

## Build and run

Requires JDK 17 and sbt 1.10.x. Docker is not required.

```bash
cd dacp-proxy-server
sbt test stage
./target/universal/stage/bin/dacp-proxy-server example.yaml
```

`example.yaml`:

```yaml
host: 0.0.0.0
port: 28815
upstream:
  host: 10.0.0.10
  port: 8815
max_connections: 256
buffer_bytes: 65536
connect_timeout_seconds: 10
idle_timeout_seconds: 0
```

`host` defaults to `127.0.0.1`; `port` defaults to `8815`. Both upstream `host` and `port` are required. An idle timeout of 0 disables it, permitting quiet streams. The connection timeout defaults to 10 seconds.

Configure the private node's public address while retaining its private listening address:

```yaml
host: 10.0.0.10
port: 8815
public_host: data.example.org
public_port: 28815
```

This is the address portion of the node configuration; supply datasets, certificates and credentials as usual. Without `public_port`, the node uses its listening port. Catalog URIs and service locations advertise the public address. Clients connect to `dacp://data.example.org:28815` using a Token issued by the private node. If clients verify certificate hostnames, the node certificate must include the public hostname.

## Transport and resource management

The relay does not decrypt TLS, persist payloads or execute operators. The upstream DACP service remains responsible for authentication and authorization; Tokens and collaboration client certificates travel end to end. The upstream sees the proxy's TCP source address. Relay logs contain the original peer, close reason and byte counts, without credentials or payloads. Counters include TLS and other protocol overhead; they are not logical Job traffic measurements.

Each connection has two fixed buffers, with nonblocking I/O and backpressure. Maximum application buffer memory is approximately `2 × buffer_bytes × max_connections`, 32 MiB by default, plus JVM, connection and operating-system overhead. The configuration caps this budget at 1 GiB. Buffer sizes range from 1 KiB to 1 MiB, and connection limits from 1 to 65536. Excess connections are rejected. Connection failures, resets and timeouts close the corresponding pair. Half-closes propagate EOF after queued bytes drain. Stopping the process closes existing connections; it does not replay requests.

Only the configured DACP port is exposed; S3 and dashboards are separate services. The relay does not rewrite URIs or aggregate datasets; configure the node's public address correctly. This mode does not provide an outbound reverse tunnel from the private network.

## Verification

`sbt test` checks large streams, backpressure, half-close, connection limits, shutdown cleanup, upstream failures, timeouts and strict configuration validation. After building, test TLS, catalog discovery, byte-stream digests and invalid Tokens against an existing node. The script starts and stops a temporary relay:

```bash
cd ..
PYTHONPATH=dacp-client python scripts/test-proxy-dacp.py \
  --upstream dacp://localhost:18861 --dataset weather --item /observations \
  --token-file /secure/node.token --ca /secure/ca.crt
```

The script requires Python SDK dependencies and uses a localhost certificate with a local node. Production clients should use the actual public hostname and a suitable node certificate.

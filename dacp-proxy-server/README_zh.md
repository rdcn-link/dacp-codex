# DACP 代理服务器

[English](README.md)

Scala 编写的单上游 TCP 透传服务。代理所在机器必须能够通过路由或 VPN 访问内网 DACP；一个实例仅代理一个服务。

```text
客户端 → 公网代理:28815 → 内网 DACP:8815
```

## 构建与启动

需要 JDK 17、sbt 1.10.x。不需要 Docker。

```bash
cd dacp-proxy-server
sbt test stage
./target/universal/stage/bin/dacp-proxy-server example.yaml
```

`example.yaml`：

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

`host` 默认 `127.0.0.1`，`port` 默认 `8815`。上游必须明确提供 `host` 和 `port`。空闲超时为 0 表示禁用，允许长时间无数据的流；连接超时默认 10 秒。

内网节点配置对外地址，监听地址仍为内网地址：

```yaml
host: 10.0.0.10
port: 8815
public_host: data.example.org
public_port: 28815
```

这是节点配置的地址部分；其余数据集、证书和凭据配置照常提供。未设置 `public_port` 时使用节点监听端口。目录 URI 和服务地址会公布公网地址。客户端连接 `dacp://data.example.org:28815`，使用内网节点签发的 Token；如客户端验证证书主机名，节点证书应包含公网域名。

## 传输与资源管理

代理不解密 TLS、不保存数据内容、不执行算子。认证与授权始终由上游 DACP 服务判断，Token 和协作客户端证书端到端传递。上游看到的 TCP 来源是代理；代理日志记录原始连接来源、关闭原因和双向字节数，不记录凭据或数据内容。字节统计包含 TLS 等协议开销，不是 Job 的逻辑数据量。

每条连接使用两个固定缓冲区，通过非阻塞 I/O 和背压持续转发。最大应用缓冲内存约为 `2 × buffer_bytes × max_connections`，默认 32 MiB，另有 JVM、连接和操作系统开销；配置上限为 1 GiB。缓冲区范围 1 KiB–1 MiB，并发连接范围 1–65536。连接满时拒绝新连接；连接失败、重置或超时会关闭对应双向连接。支持半关闭，缓冲数据排空后再传递 EOF；关闭进程会终止现有连接，不做自动重放。

只暴露配置的 DACP 端口，不代理 S3 或 Web 看板。代理不修改 URI、不聚合数据集；需要正确配置节点的公开地址。此模式不提供内网主动拨出的反向隧道。

## 验证

`sbt test` 检查大流、背压、半关闭、连接上限、关闭清理、上游故障、超时和严格配置校验。构建后可使用已有节点做 TLS、目录、字节流摘要及无效 Token 验证；测试会自动启动并停止一个临时代理：

```bash
cd ..
PYTHONPATH=dacp-client python scripts/test-proxy-dacp.py \
  --upstream dacp://localhost:18861 --dataset weather --item /observations \
  --token-file /secure/node.token --ca /secure/ca.crt
```

该脚本需要 Python SDK 的依赖，使用 `localhost` 证书测试本机节点。生产环境应使用实际公开域名和适配的节点证书。

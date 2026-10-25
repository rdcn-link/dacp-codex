# DACP Scala collaboration SDK

[中文](README.md)

For Union-to-member service calls, published separately from ordinary `dacp-client`. Both share Flight connections, credential transport and stream protocol code. Servers enforce authorization; using this SDK grants no additional permissions.

```bash
sbt test publishLocal
```

```scala
import dacp.collaboration.CollaborationClient
import dacp.union.Tls
import java.nio.file.{Files, Path}

val tls = new Tls(Path.of("union.crt"), Path.of("union.key"), Path.of("ca.crt"))
val member = new CollaborationClient(tls, credential = origin =>
  if (origin == "dacp://localhost:18861")
    Some(Files.readString(Path.of("/protected/a-union.token")).trim)
  else None)
try println(member.membership("dacp://localhost:18861", "YOUR_UNION_ID"))
finally member.close()
```

The target node must issue the credential, bound to the Union ID and certificate fingerprint. Exactly authorized peer task streams carry no Union credential and do not read ordinary client token environment variables. Interfaces cover member status, prepare/start, execution status, cancellation, materialization status and output streams. Outputs are BinarySource instances that must be closed.

See [authentication and access control](../docs/security_en.md).

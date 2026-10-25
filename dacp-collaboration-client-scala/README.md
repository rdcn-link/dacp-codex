# DACP Scala 协作 SDK

[English](README_en.md)

供 Union 与成员节点之间的服务调用使用，与普通 `dacp-client` 分开发布。二者共享 Flight 连接、凭据传输和流协议代码。服务端执行授权，使用本 SDK 不会获得额外权限。

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

凭据必须由目标节点签发并绑定 Union ID 与证书指纹。节点间精确授权的任务流连接不携带 Union 凭据，也不从普通客户端环境变量读取 Token。接口包括成员状态、prepare/start、执行状态、取消、物化状态与输出流；输出是需要关闭的BinarySource。

详见[认证与访问控制](../docs/security.md)。

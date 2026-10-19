# Union 传输协议模块

[English](README_en.md)

为 `dacp-server`、`dacp-union-server` 和 Scala 客户端提供共享 JSON 校验、持久化工具、Arrow Flight mTLS 监听和请求工具。业务身份为 TLS 验证后的叶证书 SHA-256；可固定远端证书指纹。JSON 消息上限为 1 MiB。

此模块不包含数据后端、文件格式或领域算子。详细接口及运行方式见 [Union 服务器](../dacp-union-server/README.md)，授权和数据流约定见 [协议文档](../docs/union.md)。`dacp-union-server` 的契约测试通过真实 Arrow Flight mTLS 验证此模块。

# DACP Python 协作 SDK

[English](README_en.md)

供 Union 与成员节点之间的服务调用使用，与普通 `dacp-client` 分开发布。二者共享 Flight 连接、凭据传输和流协议代码。服务端执行授权，使用本 SDK 不会获得额外权限。

```bash
python -m pip install ./dacp-client ./dacp-collaboration-client
```

```python
from dacp_collaboration import CollaborationClient

with CollaborationClient('dacp://localhost:18861',
        ca='ca.crt', cert='union.crt', key='union.key',
        credential_file='/protected/a-union.token') as member:
    print(member.membership('YOUR_UNION_ID'))
    print(member.node_status('YOUR_UNION_ID'))
```

凭据必须由目标节点签发并绑定 Union ID 与证书指纹。节点间精确授权的任务流连接不携带 Union 凭据，也不从普通客户端环境变量读取 Token。接口包括成员状态、prepare/start、执行状态、取消、物化状态与输出流；输出是需要关闭的原始 Flight reader。

详见[认证与访问控制](../docs/security.md)。

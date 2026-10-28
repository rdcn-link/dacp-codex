# DACP Python collaboration SDK

[中文](README_zh.md)

For Union-to-member service calls, published separately from ordinary `dacp-client`. Both share Flight connections, credential transport and stream protocol code. Servers enforce authorization; using this SDK grants no additional permissions.

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

The target node must issue the credential, bound to the Union ID and certificate fingerprint. Exactly authorized peer task streams carry no Union credential and do not read ordinary client token environment variables. Interfaces cover member status, prepare/start, execution status, cancellation, materialization status and output streams. Outputs are raw Flight readers that must be closed.

See [authentication and access control](../docs/security.md).

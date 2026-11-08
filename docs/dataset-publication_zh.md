# 数据集发布策略

发布策略控制整个数据集的远程内容读取，所有 Item 和所有 View 统一继承。数据集描述、Item 与 Folder 列表、声明性 schema、大小、时间等元数据始终通过经过身份验证的 TLS 匿名公开。消耗内容的显式扫描、查询、预览和采样属于读取。

在数据集的公开描述 JSON 中设置 `publication`。该方式适用于目录插件，Union 重命名数据集时保留原策略。配置示例：

```yaml
datasets:
  observations:
    provider: directory
    description: '{"title":"Observations","description":"Example dataset","publication":{"policy":"embargoed","public_at":"2027-01-01T00:00:00Z"}}'
    source:
      root: /srv/data/observations
```

请使用所选插件支持的 source 配置；发布策略不改变后端契约。

| 策略 | 远程内容读取 |
| --- | --- |
| `public`，公开 | 允许匿名读取 |
| `private`，不公开 | 禁止普通远程读取 |
| `embargoed`，定时公开 | `public_at` 之前禁止读取；达到该时间后允许匿名读取 |

`public_at` 使用带时区偏移的 ISO 8601 时间，如 `2027-01-01T00:00:00Z`。以服务器时钟为准，到期无需重启。公开和不公开策略只接受 `policy`；定时公开必须同时包含且仅包含 `policy`、`public_at`。未声明策略时默认不公开，非法策略拒绝访问。需要公开的已有数据集应显式增加 `{"policy":"public"}`。

DACP 各 View 的读取、计算记录数、S3 GET（包括签名请求），以及远程 Job 的 `read_item` 输入均遵循同一策略。S3 列表和 HEAD 不读取内容，匿名公开。提供的凭据或签名无效时，即使访问公开操作也会被拒绝。reader、runner 和预签名链接不能绕过不公开或尚未到期的策略。提交 Job 仍需 runner 权限，结果接口仍校验任务归属。仅使用公开元数据的结构推导可由具有推导权限的客户端调用；实际读取内容的推导必须满足发布策略。

节点完全信任经过显式认证的 Union 服务身份，此类请求可以读取不公开数据。Union 在代理读取或下发 Job 前，对普通用户应用源数据集策略，无需将 Union 用户映射为节点账号。不得向用户分发 Union 服务凭据。内部服务凭据仅保存在进程中，用于请求准入后的内部操作。受信任的本地嵌入式计算与普通远程访问分开。

在请求或 Job 输入检查时判断读取准入；仅普通策略变化不会打断已准入的数据流，Union 凭据撤销仍保持原有取消行为。新准入会读取目录元数据，Union 目录缓存可能延迟策略的可见性。这里不提供 Item 级 ACL 或逐用户授权。

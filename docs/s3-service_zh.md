# S3 兼容数据读取服务

[English](s3-service.md)

DACP Server 默认同时启动 Flight、Web 看板和一个独立的 HTTPS S3 兼容端口。S3 服务复用实时 DataCatalog 与 DatasetAccess：dataset 对应 bucket，Item 的逻辑路径去掉开头的 `/` 后对应 object key。例如 `climate` 中的 `/observations/day.csv` 对应 `s3://climate/observations/day.csv`。不复制数据，不产生第二份目录配置。

## 配置与生命周期

```yaml
host: 0.0.0.0
public_host: localhost
port: 8815
s3:
  enabled: true
  host: 0.0.0.0
  port: 10815
```

这段配置应与现有 datasets 或 data_catalog、TLS 等节点配置合并使用。省略 `s3` 时默认启用，绑定 `0.0.0.0`，端口为实际 Flight 监听端口加 2000；Flight 端口超过 63535 时需明确设置 S3 端口。`port: 0` 由系统分配端口。`s3: {enabled: false}` 关闭服务。`web.enabled` 与 S3 相互独立。

服务随 `DacpServer.start()` 启动，随 `close()` 关闭；绑定失败会回滚整个节点启动。嵌入式应用通过 `server.s3Port` 获取实际端口。启动日志输出 S3 监听地址；通过 `http://节点主机:S3端口/` 访问。

S3 是独立认证的只读 HTTPS 端点，使用节点 TLS 证书。所有读取（含列表和元数据）要求 SigV4 认证，不支持匿名访问。密钥与 Flight Token 独立，通过本地凭据工具创建 kind=s3 的密钥；密钥撤销拒绝后续请求，已接纳的下载继续。支持预签名 GET 临时分享对象，不支持预签名列表请求。

## 数据表示与插件

- 列出并读取支持 `bytes` 视图的 Item。插件仅提供 `records` 时，节点的通用 Item 适配层自动提供 UTF-8 JSONL `bytes` 视图，因此这些 Item 也可从 S3 下载。这不是 S3 专用转换；Flight 和任务输入使用相同规则。插件已有的 `bytes` 视图优先，保持原有内容。
- 普通文件按原始字节传输，包括 CSV、图片、压缩包和 NetCDF 文件；科学文件保持原有维度、分组与变量结构。具有 `bytes` 视图的结构根也可作为完整对象下载。
- 普通逻辑文件夹不是对象；`prefix` 与 `delimiter` 可按路径分层浏览。
- 每次请求在线读取目录和元数据，不维护持久化缓存。读取结束、失败或取消时释放请求级 Dataset 租约。
- 使用 path-style 地址：`https://host:port/bucket/key`，不支持虚拟主机 bucket 地址。保留 Dataset 原名；为兼容 AWS CLI/SDK 的 bucket 校验，建议使用符合 S3 bucket 命名规则的数据集名。

S3 是核心传输适配层，不是存储后端插件。已有 Dataset provider 无需注册第二个实现；外部插件按 [Dataset 访问契约](dataset-access_zh.md) 注册，提供递归 `list()`、`describe`、`bytes` 元数据和流。声明插件原生支持的 `Item.views`，如 `Some(Seq("bytes"))` 或 `Some(Seq("records"))`；节点会为后者补充 JSONL bytes。尽可能提供 `totalSizeBytes` 与 `lastModifiedTimeMs`；内容流使用 `PluginSchemas.bytes`，每批有界分块，遵守 `ready()` 检查，在 `finally` 释放资源。无需修改 S3 代码或客户端。JSONL 回退的大小未知，Item key 不添加 `.jsonl` 后缀；完整语义见 [通用 JSONL bytes 视图](dataset-access_zh.md#通用-jsonl-bytes-视图)。未知大小的对象使用 HTTP chunked 下载，列表不虚构 Size，HEAD 不提供 Content-Length；依赖已知大小的托管下载工具可能无法处理这类对象。

## 客户端示例

在服务器本地创建密钥，妥善保存返回的访问密钥 ID 与仅显示一次的秘密：

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials create --subject download-user --kind s3
```

```python
import os
import boto3
from botocore.config import Config

s3 = boto3.client(
    's3', endpoint_url='https://localhost:10815', region_name='us-east-1',
    aws_access_key_id=os.environ['AWS_ACCESS_KEY_ID'],
    aws_secret_access_key=os.environ['AWS_SECRET_ACCESS_KEY'],
    verify='examples/certs/ca.crt',
    config=Config(signature_version='s3v4', s3={'addressing_style': 'path'}),
)
print(s3.list_buckets())
response = s3.get_object(Bucket='climate', Key='observations/day.csv')
try:
    for chunk in response['Body'].iter_chunks(chunk_size=65536):
        print(len(chunk))
finally:
    response['Body'].close()
url = s3.generate_presigned_url('get_object',
    Params={'Bucket': 'climate', 'Key': 'observations/day.csv'}, ExpiresIn=300)
```

AWS CLI 使用同样的访问密钥环境变量，并指定 `--endpoint-url https://localhost:10815` 与 `--ca-bundle examples/certs/ca.crt`，不要使用 `--no-sign-request`。也可以通过本地工具生成预签名 URL：

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials presign ACCESS_KEY_ID https://localhost:10815/climate/observations/day.csv 300
```


## 支持范围与限制

| 操作 | 行为 |
|---|---|
| ListBuckets、HeadBucket、GetBucketLocation | 实时 Dataset 目录；region 为空，客户端可指定 us-east-1 |
| ListObjects、ListObjectsV2 | prefix、delimiter、max-keys（0–1000）、marker、start-after、continuation-token、encoding-type=url |
| GetObject、HeadObject | bytes 下载及元数据；HEAD 不读取完整内容 |
| 单段 Range | 已知大小时支持闭区间、开放结束与后缀范围；非法范围返回 416 |
| 写入、删除、创建 bucket | 405 MethodNotAllowed |
| 版本、ACL、对象条件请求等未实现读取 | 501 NotImplemented |

Range 当前通过通用 bytes 流顺序跳过前段数据，读满目标范围即停止；无需落盘或缓存整个对象，但不承诺后端随机定位效率。未知大小的 Range 请求返回 501。没有自动计算 ETag、MD5 或快照版本；正在修改的数据不保证跨请求一致，分页期间目录变更也可能影响结果。对稳定下载有要求时应由插件提供稳定的数据版本。

S3 请求复用节点的轮转 JSON Lines [访问日志](access-logs_zh.md)，记录 `s3_get` / `s3_head` 等操作、dataset、Item、状态、传输字节及耗时。使用有界线程池处理下载，网络写入形成背压，关闭节点时中止活动连接。

## 验证

```bash
# 从项目根目录执行
python3 -m pip install boto3
cd dacp-server
DACP_S3_TEST_PYTHON="$(command -v python3)" sbt 'testOnly dacp.S3ServiceSpec dacp.DacpServerSpec'
```

契约测试覆盖第三方 Dataset 接口、内置文件插件、动态目录、编码、分页、范围、空对象、未知大小、资源释放，以及真实 boto3 的托管下载。未设置 `DACP_S3_TEST_PYTHON` 时，仅跳过 boto3 集成测试。

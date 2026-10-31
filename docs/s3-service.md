# S3-compatible data reading service

[中文](s3-service_zh.md)

DACP Server starts Flight, its Web dashboard, and a separate HTTPS S3-compatible endpoint by default. S3 reuses the live DataCatalog and DatasetAccess contracts: each dataset is a bucket, and an Item's logical path without its leading `/` is the object key. For example, `/observations/day.csv` in `climate` becomes `s3://climate/observations/day.csv`. Data is not copied, and no second catalog configuration is needed.

## Configuration and lifecycle

```yaml
host: 0.0.0.0
public_host: localhost
port: 8815
s3:
  enabled: true
  host: 0.0.0.0
  port: 10815
```

Merge this snippet with the node's existing datasets or data_catalog, TLS, and other settings. Omitting `s3` enables the endpoint on `0.0.0.0`, using the actual bound Flight port plus 2000. Set an explicit S3 port when Flight uses a port above 63535. `port: 0` requests an OS-assigned port. `s3: {enabled: false}` disables the service. `web.enabled` and S3 are independent.

The service starts with `DacpServer.start()` and stops with `close()`. A bind failure rolls back the entire node startup. Embedded applications can obtain the actual port through `server.s3Port`. Startup logs report the S3 listening address; access it at `https://node-host:S3-port/`.

This is an independently authenticated read-only HTTPS endpoint using the node TLS certificate. All reads, including listings and metadata, require SigV4. Anonymous access is unsupported. S3 keys are independent of Flight tokens. Create kind=s3 credentials using the local credential tool. Revocation rejects subsequent requests while admitted downloads continue. Presigned GET supports temporary object sharing; presigned listings are unsupported.

## Representations and plugins

- Items supporting the `bytes` view are listed and readable. For a records-only plugin Item, the node's common Item adapter automatically exposes UTF-8 JSONL bytes, making it downloadable through S3 too. This conversion also applies to Flight and task inputs. Existing plugin-provided bytes take priority and retain their content.
- Files stream their original bytes, including CSV, images, archives, and NetCDF. Scientific files retain dimensions, groups, and variables. A structured root advertising `bytes` is also downloadable as a complete object.
- Ordinary logical folders are not objects. Use `prefix` and `delimiter` for hierarchical browsing.
- Every request reads the live catalog and metadata without a persistent cache. Request-scoped Dataset leases are released after completion, failure, or cancellation.
- Use path-style URLs: `https://host:port/bucket/key`; virtual-host bucket addresses are unsupported. Dataset names are preserved. For compatibility with AWS CLI/SDK bucket validation, choose dataset names that satisfy S3 bucket naming rules.

S3 is a core transport adapter, not a storage backend plugin. Existing Dataset providers need no second registration. External plugins register through the [Dataset access contract](dataset-access.md), implementing recursive `list()`, `describe`, bytes metadata, and streaming. Declare the plugin's native `Item.views`, such as `Some(Seq("bytes"))` or `Some(Seq("records"))`; the node adds JSONL bytes for the latter. Supply `totalSizeBytes` and `lastModifiedTimeMs` where possible. Stream bounded batches using `PluginSchemas.bytes`, honor `ready()` checks, and release resources in `finally`. No changes to S3 code or clients are required. JSONL fallback has unknown size and does not add a `.jsonl` suffix to the key; see the [common JSONL bytes view](dataset-access.md#common-jsonl-bytes-view). Unknown-size objects use chunked HTTP downloads, omit Size from listings, and omit Content-Length on HEAD; managed download tools requiring a known size may not support these objects.

## Client examples

Create a key locally, preserving its access key ID and one-time secret securely:

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

AWS CLI uses the same access key environment variables, `--endpoint-url https://localhost:10815` and `--ca-bundle examples/certs/ca.crt`. Do not use `--no-sign-request`. Local presigning is also available:

```bash
target/universal/stage/bin/dacp-server credentials /protected/node-credentials presign ACCESS_KEY_ID https://localhost:10815/climate/observations/day.csv 300
```

## Supported operations and limitations

| Operation | Behavior |
|---|---|
| ListBuckets, HeadBucket, GetBucketLocation | Live Dataset catalog; empty region, clients may specify us-east-1 |
| ListObjects, ListObjectsV2 | prefix, delimiter, max-keys (0–1000), marker, start-after, continuation-token, encoding-type=url |
| GetObject, HeadObject | Bytes downloads and metadata; HEAD does not read the complete content |
| Single Range | Closed, open-ended, and suffix ranges for known-size objects; invalid ranges return 416 |
| Writes, deletion, bucket creation | 405 MethodNotAllowed |
| Unimplemented reads, including versions, ACLs, and conditional object requests | 501 NotImplemented |

Ranges currently skip the preceding bytes in the generic stream and stop once the requested range is read. They need neither staging nor full-object buffering, but do not promise random-access backend efficiency. Ranges for unknown-size objects return 501. ETags, MD5 hashes, and snapshot versions are not computed automatically. Mutable data is not guaranteed consistent across requests, and catalog changes during pagination can affect results. Plugins should provide stable data versions when consistent downloads are required.

S3 requests use the node's rotating JSON Lines [access logs](access-logs.md), recording operations such as `s3_get` / `s3_head`, dataset, Item, status, transferred bytes, and elapsed time. A bounded thread pool handles downloads, network writes provide backpressure, and node shutdown aborts active connections.

## Verification

```bash
# Run from the repository root
python3 -m pip install boto3
cd dacp-server
DACP_S3_TEST_PYTHON="$(command -v python3)" sbt 'testOnly dacp.S3ServiceSpec dacp.DacpServerSpec'
```

Contract tests cover third-party Dataset interfaces, the built-in file plugin, live catalogs, encoding, pagination, ranges, empty objects, unknown sizes, resource cleanup, and managed downloads with a real boto3 client. Only the boto3 integration test is skipped when `DACP_S3_TEST_PYTHON` is unset.

## S3 operations overview

The S3 service page at /s3 reads /api/s3 to show enabled/running state, endpoint as plain text, startup time, uptime, active requests, request method and HTTP response counts, successful/failed requests, mean completed-request time, and object bytes sent. Transfer and request rates are differences between two browser samples. Counters are in-memory aggregates since S3 startup and reset on restart; concurrent snapshots are approximate. Object bytes count successful payload writes, including ranges and partial transfers, and exclude XML listing/error bodies, HTTP and TLS overhead. A failed transfer can have an HTTP 200/206 header, so failure counters and status-code counts describe different aspects. No credentials, bucket/object names or Item paths are included. Reading these metrics never opens a dataset or consumes a data stream. The page does not provide S3 browsing, downloads, signing or credential-management controls.

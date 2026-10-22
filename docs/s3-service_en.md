# S3-compatible data reading service

[中文](s3-service.md)

DACP Server starts Flight, its Web dashboard, and a separate HTTP S3-compatible endpoint by default. S3 reuses the live DatasetCatalog and DatasetAccess contracts: each dataset is a bucket, and an Item's logical path without its leading `/` is the object key. For example, `/observations/day.csv` in `climate` becomes `s3://climate/observations/day.csv`. Data is not copied, and no second catalog configuration is needed.

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

Merge this snippet with the node's existing datasets or dataset_catalog, TLS, and other settings. Omitting `s3` enables the endpoint on `0.0.0.0`, using the actual bound Flight port plus 2000. Set an explicit S3 port when Flight uses a port above 63535. `port: 0` requests an OS-assigned port. `s3: {enabled: false}` disables the service. `web.enabled` and S3 are independent.

The service starts with `DacpServer.start()` and stops with `close()`. A bind failure rolls back the entire node startup. Embedded applications can obtain the actual port through `server.s3Port`. Startup logs report the S3 listening address; access it at `http://node-host:S3-port/`.

This is a public, anonymous, read-only HTTP endpoint; it does not use Flight client certificates. All readable bytes Items are anonymously downloadable. To restrict access, disable it, bind it to a private interface, or configure HTTPS and access control at a reverse proxy. There is currently no S3 access-key/secret-key store. Requests carrying Authorization or `X-Amz-*` signing parameters return `AccessDenied`; clients must use unsigned / no-sign-request mode.

## Representations and plugins

- Items supporting the `bytes` view are listed and readable. For a records-only plugin Item, the node's common Item adapter automatically exposes UTF-8 JSONL bytes, making it downloadable through S3 too. This conversion also applies to Flight and task inputs. Existing plugin-provided bytes take priority and retain their content.
- Files stream their original bytes, including CSV, images, archives, and NetCDF. Scientific files retain dimensions, groups, and variables. A structured root advertising `bytes` is also downloadable as a complete object.
- Ordinary logical folders are not objects. Use `prefix` and `delimiter` for hierarchical browsing.
- Every request reads the live catalog and metadata without a persistent cache. Request-scoped Dataset leases are released after completion, failure, or cancellation.
- Use path-style URLs: `http://host:port/bucket/key`; virtual-host bucket addresses are unsupported. Dataset names are preserved. For compatibility with AWS CLI/SDK bucket validation, choose dataset names that satisfy S3 bucket naming rules.

S3 is a core transport adapter, not a storage backend plugin. Existing Dataset providers need no second registration. External plugins register through the [Dataset access contract](dataset-access_en.md), implementing recursive `list()`, `describe`, bytes metadata, and streaming. Declare the plugin's native `Item.views`, such as `Some(Seq("bytes"))` or `Some(Seq("records"))`; the node adds JSONL bytes for the latter. Supply `totalSizeBytes` and `lastModifiedTimeMs` where possible. Stream bounded batches using `PluginSchemas.bytes`, honor `ready()` checks, and release resources in `finally`. No changes to S3 code or clients are required. JSONL fallback has unknown size and does not add a `.jsonl` suffix to the key; see the [common JSONL bytes view](dataset-access_en.md#common-jsonl-bytes-view). Unknown-size objects use chunked HTTP downloads, omit Size from listings, and omit Content-Length on HEAD; managed download tools requiring a known size may not support these objects.

## Client examples

List buckets and objects, download, and read a range:

```bash
curl http://localhost:10815/
curl 'http://localhost:10815/climate?list-type=2&prefix=observations/'
curl http://localhost:10815/climate/observations/day.csv -o day.csv
curl -H 'Range: bytes=0-1023' http://localhost:10815/climate/observations/day.csv

aws --no-sign-request --endpoint-url http://localhost:10815 s3api list-buckets
aws --no-sign-request --endpoint-url http://localhost:10815 s3api list-objects-v2 --bucket climate
aws --no-sign-request --endpoint-url http://localhost:10815 s3 cp s3://climate/observations/day.csv ./day.csv
```

Python:

```python
import boto3
from botocore import UNSIGNED
from botocore.config import Config

s3 = boto3.client(
    's3', endpoint_url='http://localhost:10815', region_name='us-east-1',
    config=Config(signature_version=UNSIGNED, s3={'addressing_style': 'path'}),
)
for page in s3.get_paginator('list_objects_v2').paginate(Bucket='climate'):
    for item in page.get('Contents', []):
        print(item['Key'], item.get('Size'))
response = s3.get_object(Bucket='climate', Key='observations/day.csv')
try:
    for chunk in response['Body'].iter_chunks(chunk_size=65536):
        print(len(chunk))
finally:
    response['Body'].close()
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

S3 requests use the node's rotating JSON Lines [access logs](access-logs_en.md), recording operations such as `s3_get` / `s3_head`, dataset, Item, status, transferred bytes, and elapsed time. A bounded thread pool handles downloads, network writes provide backpressure, and node shutdown aborts active connections.

## Verification

```bash
# Run from the repository root
python3 -m pip install boto3
cd dacp-server
DACP_S3_TEST_PYTHON="$(command -v python3)" sbt 'testOnly dacp.S3ServiceSpec dacp.DacpServerSpec'
```

Contract tests cover third-party Dataset interfaces, the built-in file plugin, live catalogs, encoding, pagination, ranges, empty objects, unknown sizes, resource cleanup, and managed downloads with a real boto3 client. Only the boto3 integration test is skipped when `DACP_S3_TEST_PYTHON` is unset.

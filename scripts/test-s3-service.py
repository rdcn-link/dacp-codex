#!/usr/bin/env python3
"""Read-only S3 client contract test. Requires boto3; endpoint must serve test fixture bytes."""
import io
import os
import urllib.request
import sys

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

endpoint, bucket, key = sys.argv[1:]
s3 = boto3.client(
    "s3", endpoint_url=endpoint, region_name="us-east-1",
    aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
    verify=os.environ["AWS_CA_BUNDLE"],
    config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
)
assert bucket in [b["Name"] for b in s3.list_buckets()["Buckets"]]
s3.head_bucket(Bucket=bucket)
assert s3.get_bucket_location(Bucket=bucket)["LocationConstraint"] is None
keys = [obj["Key"] for page in s3.get_paginator("list_objects_v2").paginate(
    Bucket=bucket, PaginationConfig={"PageSize": 1}
) for obj in page.get("Contents", [])]
assert key in keys, keys
assert s3.head_object(Bucket=bucket, Key=key)["ContentLength"] == 10
body = s3.get_object(Bucket=bucket, Key=key)["Body"]
try:
    assert body.read() == b"0123456789"
finally:
    body.close()
body = s3.get_object(Bucket=bucket, Key=key, Range="bytes=2-5")["Body"]
try:
    assert body.read() == b"2345"
finally:
    body.close()
output = io.BytesIO()
s3.download_fileobj(bucket, key, output)
assert output.getvalue() == b"0123456789"
try:
    s3.put_object(Bucket=bucket, Key="forbidden", Body=b"x")
except ClientError as error:
    assert error.response["Error"]["Code"] == "MethodNotAllowed"
else:
    raise AssertionError("write unexpectedly accepted")
print("boto3: discovery, pagination, HEAD, GET, range, managed download and write rejection passed")

url = s3.generate_presigned_url('get_object', Params={'Bucket': bucket, 'Key': key}, ExpiresIn=60)
import ssl
with urllib.request.urlopen(url, context=ssl.create_default_context(cafile=os.environ['AWS_CA_BUNDLE'])) as response:
    assert response.read() == b'0123456789'
print('boto3: presigned GET passed')

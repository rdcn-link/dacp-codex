#!/usr/bin/env python3
"""Upload a locally published operator and its content-addressed blobs, using a bearer token."""
import argparse
import http.client
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit


def upload(catalog, reference, origin, token):
    url = urlsplit(origin)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password
            or url.path not in ('', '/') or url.query or url.fragment):
        raise ValueError('repository URL must be an HTTP(S) origin without credentials')
    if url.scheme != 'https' and url.hostname not in ('localhost', '127.0.0.1', '::1'):
        raise ValueError('use HTTPS for remote publication')
    parts = reference.split('@')
    if len(parts) != 2 or not all(re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', p) for p in parts):
        raise ValueError('operator must be name@version')
    data = (catalog / parts[0] / parts[1] / 'manifest.json').read_bytes()
    if len(data) > 65536:
        raise ValueError('manifest exceeds 64 KiB')
    manifest = json.loads(data)
    artifacts = [manifest['runtime']['archive']]
    if 'artifact' in manifest:
        artifacts.append(manifest['artifact'])
    artifacts += [manifest['provenance'][key] for key in ('source_archive', 'license_file', 'sbom')
                  if key in manifest.get('provenance', {})]

    def send(method, path, content, size, kind):
        connection = (http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection)(
            url.hostname, url.port, timeout=300)
        try:
            connection.putrequest(method, path)
            connection.putheader('Authorization', 'Bearer ' + token)
            connection.putheader('Content-Type', kind)
            connection.putheader('Content-Length', str(size))
            connection.endheaders()
            while True:
                chunk = content.read(1024 * 1024)
                if not chunk:
                    break
                connection.send(chunk)
            response = connection.getresponse()
            detail = response.read(4096).decode('utf-8', errors='replace')
            if response.status != 201:
                raise RuntimeError(f'publication failed: HTTP {response.status}: {detail.strip()}')
        finally:
            connection.close()

    uploaded = set()
    for artifact in artifacts:
        digest = artifact['sha256']
        if not re.fullmatch('[a-f0-9]{64}', digest):
            raise ValueError('invalid blob digest')
        if digest in uploaded:
            continue
        file = catalog / 'blobs' / 'sha256' / digest
        with file.open('rb') as content:
            send('PUT', '/v1/blobs/sha256/' + digest, content, file.stat().st_size, 'application/octet-stream')
        uploaded.add(digest)
    import io
    send('POST', '/v1/operators', io.BytesIO(data), len(data), 'application/json')
    print(f'Published {reference} to {origin.rstrip("/")}')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('operator', help='name@version')
    parser.add_argument('--catalog', type=Path, required=True)
    parser.add_argument('--url', required=True)
    parser.add_argument('--token-env', default='DACP_REPOSITORY_PUBLISH_TOKEN')
    args = parser.parse_args()
    token = os.environ.get(args.token_env)
    if not token:
        parser.error('missing publisher token environment variable')
    upload(args.catalog, args.operator, args.url, token)


if __name__ == '__main__':
    main()

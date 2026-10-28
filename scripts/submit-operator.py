#!/usr/bin/env python3
"""Submit operator sources for repository-side analysis, isolated build and administrator review."""
import argparse
import hashlib
import http.client
import io
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit
import yaml


def submit(definition, source, origin, token):
    url = urlsplit(origin)
    if (url.scheme not in ('http', 'https') or not url.hostname or url.username or url.password
            or url.path not in ('', '/') or url.query or url.fragment):
        raise ValueError('repository URL must be an HTTP(S) origin')
    if url.scheme != 'https' and url.hostname not in ('localhost', '127.0.0.1', '::1'):
        raise ValueError('remote submission requires HTTPS')
    if not token or any(c.isspace() for c in token):
        raise ValueError('invalid submitter Token')
    connection_type = http.client.HTTPSConnection if url.scheme == 'https' else http.client.HTTPConnection

    def request(method, path, content=None, kind='application/json', size=0, expected=200):
        connection = connection_type(url.hostname, url.port, timeout=300)
        try:
            connection.putrequest(method, path)
            if method != 'GET':
                connection.putheader('Authorization', 'Bearer ' + token)
                connection.putheader('Content-Type', kind)
                connection.putheader('Content-Length', str(size))
            connection.endheaders()
            if content is not None:
                for chunk in iter(lambda: content.read(1024 * 1024), b''):
                    connection.send(chunk)
            response = connection.getresponse()
            data = response.read(1024 * 1024)
            if response.status != expected:
                raise RuntimeError(f'HTTP {response.status}: {data[:4096].decode(errors="replace").strip()}')
            return json.loads(data)
        finally:
            connection.close()

    if definition.stat().st_size > 65536:
        raise ValueError('definition exceeds 64 KiB')
    class UniqueLoader(yaml.SafeLoader):
        pass
    def mapping(loader, node):
        values = {}
        for key, value in node.value:
            key = loader.construct_object(key)
            if key in values:
                raise ValueError('duplicate YAML key')
            values[key] = loader.construct_object(value)
        return values
    UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, mapping)
    n = yaml.load(definition.read_text(), Loader=UniqueLoader)
    allowed = {'name', 'version', 'description', 'icon', 'inputs', 'parameters', 'language', 'entrypoint', 'runtime', 'source_public', 'provenance'}
    if not isinstance(n, dict) or not set(n).issubset(allowed):
        raise ValueError('unsupported definition fields; do not provide artifacts or classifications')
    if not isinstance(n.get('source_public'), bool):
        raise ValueError('source_public must explicitly be true or false')
    languages = {r['language']: r['executor'] for r in request('GET', '/v1/build-languages')}
    language = n['language']
    if language not in languages:
        raise ValueError('choose a build language supported by the repository plugins')
    reference = n['runtime']
    runtimes = request('GET', '/v1/runtimes')
    runtime = next((r for r in runtimes if r['name']+'@'+r['version'] == reference), None)
    if runtime is None or language not in runtime['build']['languages']:
        raise ValueError('choose a compatible runtime from /v1/runtimes')

    def upload(path):
        if not re.fullmatch(r'[A-Za-z0-9_-][A-Za-z0-9_.-]*', path.name):
            raise ValueError('invalid source-material filename')
        if not 0 < path.stat().st_size <= 64 * 1024 * 1024:
            raise ValueError('source materials must be nonempty and at most 64 MiB')
        digest = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(1024 * 1024), b''):
                digest.update(block)
        value = digest.hexdigest()
        with path.open('rb') as stream:
            request('PUT', '/v1/blobs/sha256/'+value, stream, 'application/octet-stream', path.stat().st_size, 201)
        return {'sha256': value, 'size': path.stat().st_size, 'filename': path.name}

    provenance = dict(n.get('provenance', {}))
    for key in ('license_file', 'sbom'):
        if key in provenance:
            provenance[key] = upload(definition.parent / provenance[key])
    if 'source_archive' in provenance:
        raise ValueError('provide source using --source, not provenance.source_archive')
    provenance['source_archive'] = upload(source)
    manifest = {key: n[key] for key in ('name', 'version', 'description', 'icon', 'inputs', 'parameters', 'source_public')}
    manifest.update(api_version=2, execution={'executor': languages[language], 'entrypoint': n['entrypoint'], 'config': {}},
                    runtime={'image': runtime['image'], 'archive': runtime['archive']}, build={'language': language}, provenance=provenance)
    data = json.dumps(manifest, ensure_ascii=False).encode()
    result = request('POST', '/v1/submissions', io.BytesIO(data), size=len(data), expected=202)
    print(json.dumps(result))
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('definition', type=Path, help='Source submission YAML')
    parser.add_argument('--source', type=Path, required=True, help='ZIP/TAR containing src/ and optional resources/')
    parser.add_argument('--url', required=True)
    auth = parser.add_mutually_exclusive_group(required=True)
    auth.add_argument('--token-file', type=Path)
    auth.add_argument('--token-env')
    args = parser.parse_args()
    token = args.token_file.read_text().strip() if args.token_file else os.environ.get(args.token_env, '')
    submit(args.definition, args.source, args.url, token)


if __name__ == '__main__':
    main()

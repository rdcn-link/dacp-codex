"""Create a tiny wheel and catalog, reusing an existing Python runtime archive."""
from pathlib import Path
import argparse
import base64
import hashlib
import io
import json
import os
import shutil
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def archive(files):
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
        for name, data in sorted(files.items()):
            info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, data)
    return out.getvalue()


def build(catalog, output):
    runtime = json.loads((catalog / 'piflow_csv_parser/1.0/manifest.json').read_text())['runtime']
    blobs = output / 'blobs/sha256'
    blobs.mkdir(parents=True, exist_ok=True)
    def store(data, filename):
        digest = hashlib.sha256(data).hexdigest()
        (blobs / digest).write_bytes(data)
        return dict(sha256=digest, size=len(data), filename=filename)
    files = {'live_ticks.py': (HERE / 'live_ticks.py').read_bytes(),
             'dacp_operator/__init__.py': (ROOT / 'dacp-operator-sdk/python/dacp_operator/__init__.py').read_bytes()}
    dist = 'dacp_live_ticks-1.0.dist-info/'
    files[dist + 'METADATA'] = b'Metadata-Version: 2.1\nName: dacp-live-ticks\nVersion: 1.0\nRequires-Python: >=3.10\n'
    files[dist + 'WHEEL'] = b'Wheel-Version: 1.0\nGenerator: dacp\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    files[dist + 'RECORD'] = ('\n'.join(f'{n},sha256={base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode()},{len(b)}' for n,b in sorted(files.items())) + '\n' + dist + 'RECORD,,\n').encode()
    artifact = store(archive(files), 'dacp_live_ticks-1.0-py3-none-any.whl')
    source = store(archive({p.name:p.read_bytes() for p in HERE.iterdir() if p.is_file() and p.suffix in ('.py', '.md')}), 'live_ticks-sources.zip')
    original = catalog / 'blobs/sha256' / runtime['archive']['sha256']
    target = blobs / original.name
    if not target.exists():
        try: os.link(original, target)
        except OSError: shutil.copyfile(original, target)
    manifest = dict(api_version=2, name='live_ticks', version='1.0',
                    description='Continuously emit timestamped synthetic weather JSONL. Params: interval_seconds (0.01-60, default 1), max_records (0 means continuous). Consume bytes source as trigger. Cancel or timeout to stop.',
                    inputs={'source':dict(representations=['bytes'], formats=[])},
                    outputs={'result':dict(representations=['bytes'], formats=['ndjson'])},
                    execution=dict(executor='python', entrypoint='live_ticks', config={}), runtime=runtime, artifact=artifact,
                    provenance=dict(source_archive=source, source_repository='https://github.com/rdcn-link/dacp-codex', build_instructions='python3 examples/operators/live-stream/build.py --runtime-catalog CATALOG --output OUTPUT. Reuses piflow_csv_parser@1.0 Python runtime; embeds the DACP Python SDK.'))
    directory = output / 'live_ticks/1.0'
    directory.mkdir(parents=True, exist_ok=True)
    (directory / 'manifest.json').write_text(json.dumps(manifest, indent=2) + '\n')
    print('Built live_ticks@1.0:', artifact['size'], 'bytes; shared runtime', runtime['image'])


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--runtime-catalog', required=True, type=Path)
    parser.add_argument('--output', required=True, type=Path)
    args = parser.parse_args()
    build(args.runtime_catalog, args.output)

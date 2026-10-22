#!/usr/bin/env python3
"""Build and publish exactly one individually verified PiFlow-derived operator."""
import argparse
import base64
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import zipfile

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]


def mark_published(name, verification):
    import fcntl
    lock = HERE / 'target/inventory.lock'
    lock.parent.mkdir(parents=True, exist_ok=True)
    with lock.open('a') as handle:
        fcntl.flock(handle, fcntl.LOCK_EX)
        path = HERE / 'inventory.json'
        inventory = json.loads(path.read_text())
        item = next(i for i in inventory['operators'] if i['operator'] == name)
        item.update(status='published', version='1.0', verification=verification)
        temporary = path.with_suffix('.json.tmp')
        temporary.write_text(json.dumps(inventory, indent=2) + '\n')
        temporary.replace(path)


def archive(files):
    out = io.BytesIO()
    with zipfile.ZipFile(out, 'w', zipfile.ZIP_DEFLATED) as z:
        for name, data in sorted(files.items()):
            info = zipfile.ZipInfo(name, (2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            z.writestr(info, data)
    return out.getvalue()


def build(name, runtime_catalog, destination):
    specs = json.loads((HERE / 'specs.json').read_text())
    spec = specs[name]
    inventory = json.loads((HERE / 'inventory.json').read_text())
    item = next(x for x in inventory['operators'] if x['upstream_class'].endswith('.' + spec['class']))
    runtime = json.loads((runtime_catalog / 'identity_python/1.1/manifest.json').read_text())['runtime']
    blobs = destination / 'blobs/sha256'
    blobs.mkdir(parents=True, exist_ok=True)

    def store(data, filename):
        digest = hashlib.sha256(data).hexdigest()
        (blobs / digest).write_bytes(data)
        return dict(sha256=digest, size=len(data), filename=filename)

    module = 'dacp_piflow_' + name
    files = {module + '.py': (HERE / 'operators' / (name + '.py')).read_bytes(),
             'dacp_operator/__init__.py': (ROOT / 'dacp-operator-sdk/python/dacp_operator/__init__.py').read_bytes()}
    if (HERE / 'operators/support.py').exists():
        files['dacp_piflow_support.py'] = (HERE / 'operators/support.py').read_bytes()
    if spec.get('uses_sql'):
        files['dacp_piflow_sql.py'] = (HERE / 'operators/execute_sql_stop.py').read_bytes()
    dist = module + '-1.0.dist-info/'
    if spec.get('uses_excel'):
        lock = json.loads((HERE / 'excel-dependencies.json').read_text())
        for filename, expected in lock.items():
            wheel = HERE / 'target/excel-deps' / filename
            if not wheel.is_file() or hashlib.sha256(wheel.read_bytes()).hexdigest() != expected:
                raise ValueError('download the hash-pinned Excel wheels into target/excel-deps; see README')
            with zipfile.ZipFile(wheel) as dependency:
                for entry in dependency.namelist():
                    if entry.endswith('/'):
                        continue
                    if '.dist-info/' not in entry:
                        files[entry] = dependency.read(entry)
                    elif 'LICENSE' in entry.upper():
                        files[dist + 'licenses/' + filename + '-' + Path(entry).name] = dependency.read(entry)
        files[dist + 'excel-dependencies.json'] = (HERE / 'excel-dependencies.json').read_bytes()
    files[dist + 'METADATA'] = (f'Metadata-Version: 2.1\nName: {module.replace("_", "-")}\n'
                               'Version: 1.0\nLicense: Apache-2.0\nRequires-Python: >=3.10\n'
                               'Requires-Dist: pyarrow>=23.0.1\n').encode()
    files[dist + 'WHEEL'] = b'Wheel-Version: 1.0\nGenerator: dacp-piflow\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    files[dist + 'licenses/LICENSE'] = (HERE / 'LICENSE').read_bytes()
    files[dist + 'RECORD'] = ('\n'.join(
        f'{n},sha256={base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode()},{len(b)}'
        for n, b in sorted(files.items())) + '\n' + dist + 'RECORD,,\n').encode()
    artifact = store(archive(files), module + '-1.0-py3-none-any.whl')
    source_files = {n: p.read_bytes() for n in ('README.md', 'README_en.md', 'review.md', 'review_en.md', 'LICENSE', 'NOTICE', 'inventory.json', 'specs.json', 'publish.py', 'test.py', 'container_test.py', 'integration_test.py', 'audit.py', 'excel-dependencies.json') if (p := HERE / n).exists()}
    source_files.update(files)
    source_files['upstream/' + item['source']] = (HERE / 'upstream' / item['source']).read_bytes()
    source = store(archive(source_files), module + '-sources.zip')
    license_blob = store((HERE / 'LICENSE').read_bytes(), 'LICENSE.txt')
    original = runtime_catalog / 'blobs/sha256' / runtime['archive']['sha256']
    target = blobs / original.name
    if not target.exists():
        try:
            os.link(original, target)
        except OSError:
            shutil.copyfile(original, target)
    manifest = dict(api_version=2, name=item['operator'], version='1.0',
                    description=spec['description'], inputs=spec['inputs'], outputs=spec['outputs'],
                    execution=dict(executor='python', entrypoint=module, config={}),
                    runtime=runtime, artifact=artifact,
                    provenance=dict(source_archive=source, license='Apache-2.0', license_file=license_blob,
                        source_repository=inventory['repository'], source_revision=inventory['commit'],
                        build_instructions='PiFlow EcoFlow adapted to DACP Arrow streams. Upstream commit: ' + inventory['commit'] +
                        '. See bundled README and specs.json for semantic differences and tests. Build: .venv/bin/python examples/operators/piflow/publish.py ' + name + ' --runtime-catalog BACKUP --output CATALOG.'))
    path = destination / item['operator'] / '1.0'
    path.mkdir(parents=True, exist_ok=True)
    (path / 'manifest.json').write_text(json.dumps(manifest, ensure_ascii=False))
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('name')
    parser.add_argument('--runtime-catalog', type=Path, required=True)
    parser.add_argument('--output', type=Path, default=HERE / 'target/catalog')
    parser.add_argument('--publish', help='Repository origin, e.g. http://localhost:8820')
    parser.add_argument('--token-file', type=Path, default=ROOT / '.operator-catalog/.publish-token')
    args = parser.parse_args()
    subprocess.run([sys.executable, str(HERE / 'test.py'), args.name], check=True)
    manifest = build(args.name, args.runtime_catalog, args.output)
    if args.publish:
        environment = os.environ.copy()
        environment['DACP_PIFLOW_CATALOG'] = str(args.output.resolve())
        subprocess.run([sys.executable, str(HERE / 'container_test.py'), args.name], env=environment, check=True)
        spec = importlib.util.spec_from_file_location('dacp_upload', ROOT / 'scripts/upload-operator.py')
        uploader = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(uploader)
        uploader.upload(args.output, manifest['name'] + '@1.0', args.publish, args.token_file.read_text().strip())
        mark_published(manifest['name'], 'individual Arrow SDK contract tests')
    print(json.dumps(dict(operator=manifest['name'], code_bytes=manifest['artifact']['size'], verified=True)))


if __name__ == '__main__':
    main()

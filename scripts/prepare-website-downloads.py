#!/usr/bin/env python3
"""Build current software and publish atomic website download artifacts, with SHA-256."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile

ROOT = Path(__file__).resolve().parents[1]
APPS = ('dacp-server', 'dacp-union-server')

def run(args, cwd):
    subprocess.run(args, cwd=cwd, check=True)

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def publish(destination, build=True):
    destination = destination.resolve()
    destination.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='dacp-downloads-') as tmp:
        staging = Path(tmp) / "artifacts"
        staging.mkdir()
        for module in APPS:
            if build:
                run(['sbt', 'Universal/packageBin'], ROOT / module)
            archives = sorted((ROOT / module / 'target/universal').glob('*.zip'), key=lambda p: p.stat().st_mtime)
            if not archives:
                raise RuntimeError(f'Missing {module} distribution; run without --skip-build')
            shutil.copy2(archives[-1], staging / archives[-1].name)
        python_source = Path(tmp) / 'dacp-client'
        shutil.copytree(ROOT / 'dacp-client', python_source, ignore=shutil.ignore_patterns('build', 'target', '*.egg-info', '__pycache__', '.pytest_cache', '.venv'))
        run([os.environ.get('PYTHON', 'python3'), '-m', 'pip', 'wheel', '--no-deps', '--wheel-dir', str(staging), '.'], python_source)
        sums = ''.join(f'{digest(p)}  {p.name}\n' for p in sorted(staging.iterdir()) if p.is_file())
        (staging / 'SHA256SUMS').write_text(sums)
        for p in staging.iterdir():
            partial = destination / ('.' + p.name + '.tmp')
            shutil.copy2(p, partial)
            partial.replace(destination / p.name)
    print(f'Downloads published to {destination}')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=ROOT / 'dacp-website/downloads')
    parser.add_argument('--skip-build', action='store_true', help='Use existing server application packages')
    args = parser.parse_args()
    publish(args.output, not args.skip_build)

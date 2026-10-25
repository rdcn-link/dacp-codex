#!/usr/bin/env python3
"""Build current software and publish atomic website download artifacts, with SHA-256."""
import argparse
import hashlib
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import zipfile

ROOT = Path(__file__).resolve().parents[1]
APPS = ('dacp-server', 'dacp-union-server', 'dacp-operator-repository', 'dacp-website')
SDKS = ('dacp-client-scala', 'dacp-collaboration-client-scala', 'dacp-operator-sdk')

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
        # SDK bundles contain their complete runtime classpath, not just a thin JAR.
        for module in SDKS:
            if build:
                completed = subprocess.run(['sbt', 'export Runtime/fullClasspath'], cwd=ROOT / module, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                if completed.returncode:
                    raise RuntimeError(f'{module} classpath export failed:\n{completed.stdout}')
                result = completed.stdout
                paths = [line.strip() for line in result.splitlines() if line.strip().startswith('/')]
                if not paths:
                    raise RuntimeError(f'Could not export {module} runtime classpath')
                (ROOT / module / 'target/website-classpath.txt').write_text('\n'.join(paths[-1].split(os.pathsep)))
            cp_file = ROOT / module / 'target/website-classpath.txt'
            if not cp_file.exists():
                raise RuntimeError(f'Missing {module} classpath; run without --skip-build')
            with zipfile.ZipFile(staging / f'{module}-development.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
                for line in cp_file.read_text().splitlines():
                    p = Path(line)
                    if p.is_file():
                        archive.write(p, f'{module}/lib/{p.name}')
                    elif p.is_dir():
                        for f in p.rglob('*'):
                            if f.is_file():
                                archive.write(f, f'{module}/classes/{f.relative_to(p)}')
                archive.writestr(f'{module}/INSTALL.txt', 'Development SDK bundle. JDK 17+. Add classes/ and lib/* to your JVM classpath.\nSee https://github.com/rdcn-link/dacp-codex for API documentation.\n')
        for module in ('dacp-client', 'dacp-collaboration-client'):
            python_source = Path(tmp) / module
            shutil.copytree(ROOT / module, python_source, ignore=shutil.ignore_patterns('build', 'target', '*.egg-info', '__pycache__', '.pytest_cache', '.venv'))
            run([os.environ.get('PYTHON', 'python3'), '-m', 'pip', 'wheel', '--no-deps', '--wheel-dir', str(staging), '.'], python_source)
        tracked = subprocess.check_output(['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=ROOT).decode().split('\0')
        # Include the new module before it is committed; omit generated artifacts.
        website = [str(p.relative_to(ROOT)) for p in (ROOT / 'dacp-website').rglob('*') if p.is_file() and not any(x in p.parts for x in ('target', 'downloads', '.bsp'))]
        with zipfile.ZipFile(staging / 'dacp-source-development.zip', 'w', zipfile.ZIP_DEFLATED) as archive:
            for name in sorted(set(tracked + website + ['scripts/prepare-website-downloads.py', 'scripts/test-website.py'])):
                p = ROOT / name
                if name and p.is_file():
                    archive.write(p, f'dacp/{name}')
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
    parser.add_argument('--skip-build', action='store_true', help='Use existing application packages and SDK classpath exports')
    args = parser.parse_args()
    publish(args.output, not args.skip_build)

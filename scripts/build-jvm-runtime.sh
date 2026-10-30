#!/usr/bin/env bash
# Build on Linux for the local Docker engine's architecture; no base-image pull.
set -euo pipefail
cd "$(dirname "$0")/.."
project_root="$PWD"
runtime_tag="${1:-dacp/operator-jvm:1.1}"
if [[ "$(uname -s)" != Linux ]]; then echo 'Build the operator image on Linux.' >&2; exit 2; fi
(cd operators/runtime-jvm && sbt -batch stageRuntime)
build_dir=$(mktemp -d "${TMPDIR:-/tmp}/dacp-operator-image-XXXXXX")
trap 'rm -rf "$build_dir"' EXIT
jlink --add-modules java.base,java.logging,java.management,java.naming,java.security.jgss,java.sql,java.xml,jdk.unsupported,jdk.crypto.ec,jdk.security.auth \
  --strip-debug --no-header-files --no-man-pages --compress=2 --output "$build_dir/rootfs/opt/java"
python3 - "$build_dir/rootfs" "$project_root/operators/runtime-jvm/target/runtime/lib" <<'PY'
import os, platform, re, shutil, subprocess, sys, zipfile
from pathlib import Path
root = Path(sys.argv[1])
shutil.copytree(sys.argv[2], root / 'opt/lib')
runtime = root / 'opt/java'
# Load compression JNI from the image rather than extracting into a noexec /tmp.
native = root / 'opt/native'
native.mkdir(parents=True)
arch = platform.machine()
zstd_arch = {'x86_64': 'amd64', 'aarch64': 'aarch64'}.get(arch)
if zstd_arch is None:
    raise RuntimeError('Supported image build architectures: x86_64, aarch64')
for pattern, resource_pattern, destination in [
    ('zstd-jni-*.jar', f'linux/{zstd_arch}/libzstd-jni-', 'libzstd-jni.so'),
    ('snappy-java-*.jar', f'org/xerial/snappy/native/Linux/{arch}/libsnappyjava.so', 'libsnappyjava.so')]:
    jars = list((root / 'opt/lib').glob(pattern))
    if len(jars) != 1:
        raise RuntimeError(f'Expected one codec JAR: {pattern}')
    with zipfile.ZipFile(jars[0]) as jar:
        entries = [name for name in jar.namelist() if name.startswith(resource_pattern) and name.endswith('.so')]
        if len(entries) != 1:
            raise RuntimeError(f'Missing native codec for {arch}')
        (native / destination).write_bytes(jar.read(entries[0]))
# Retain native libraries needed by the linked JVM and JNI compression codecs.
for binary in [runtime / 'bin/java', *runtime.rglob('*.so'), *native.glob('*.so')]:
    output = subprocess.run(['ldd', str(binary)], text=True, capture_output=True,
                            env={**os.environ, 'LD_LIBRARY_PATH': str(runtime / 'lib') + ':' + str(runtime / 'lib/server')})
    if 'not found' in output.stdout:
        raise RuntimeError(output.stdout)
    for value in re.findall(r'(?<!\S)(/[^\s]+)', output.stdout):
        source = Path(value)
        if source.is_relative_to(root):
            continue
        target = root / source.relative_to('/')
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists():
            shutil.copy2(source, target, follow_symlinks=True)
(root / 'tmp').mkdir(mode=0o1777)
os.chmod(root / 'tmp', 0o1777)
(root / 'etc').mkdir(exist_ok=True)
(root / 'etc/passwd').write_text('root:x:0:0:root:/tmp:/nonexistent\n')
(root / 'etc/group').write_text('root:x:0:\n')
PY
docker build -f operators/runtime-jvm/Dockerfile -t "$runtime_tag" "$build_dir"

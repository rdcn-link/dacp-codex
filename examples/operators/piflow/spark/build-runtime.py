#!/usr/bin/env python3
"""Build one shared Spark runtime without per-operator dependencies or base-image pulls."""
import argparse
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import tempfile
import zipfile

HOME = Path(__file__).resolve().parents[1]
ROOT = HOME.parents[2]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--spark-home', type=Path, required=True, help='Extracted pyspark 4.0.1 source distribution')
    args = parser.parse_args()
    jars = args.spark_home / 'deps/jars'
    assert (jars / 'spark-core_2.13-4.0.1.jar').is_file(), 'pyspark 4.0.1 distribution required'
    with tempfile.TemporaryDirectory(prefix='dacp-ecoflow-runtime-') as tmp:
        tmp = Path(tmp)
        root = tmp / 'rootfs'
        java = root / 'opt/java'
        java.parent.mkdir(parents=True)
        subprocess.run(['jlink', '--add-modules', 'java.se,jdk.unsupported,jdk.crypto.ec,jdk.security.auth,jdk.management',
            '--strip-debug', '--no-header-files', '--no-man-pages', '--compress=2', '--output', str(java)], check=True)
        # Spark needs java.beans from java.desktop, but has no graphical display.
        for name in ('libawt.so', 'libawt_headless.so', 'libawt_xawt.so', 'libjawt.so',
                     'libfontmanager.so', 'libjavajpeg.so', 'liblcms.so', 'libsplashscreen.so'):
            (java / 'lib' / name).unlink(missing_ok=True)
        lib = root / 'opt/lib'
        lib.mkdir()
        # Compiler jars are build tools, not required when executing published Scala operators.
        for p in jars.glob('*.jar'):
            if not p.name.startswith('scala-compiler-'):
                shutil.copyfile(p, lib / p.name)
        api = ROOT / 'dacp-plugin-api/target/scala-2.13/dacp-plugin-api_2.13-0.1.0-SNAPSHOT.jar'
        shutil.copyfile(api, lib / api.name)
        native = root / 'opt/native'
        native.mkdir()
        arch = platform.machine()
        for pattern, resource, output in [
            ('zstd-jni-*.jar', 'linux/' + {'x86_64': 'amd64', 'aarch64': 'aarch64'}[arch] + '/libzstd-jni-', 'libzstd-jni.so'),
            ('snappy-java-*.jar', 'org/xerial/snappy/native/Linux/' + arch + '/libsnappyjava.so', 'libsnappyjava.so')]:
            jar, = lib.glob(pattern)
            with zipfile.ZipFile(jar) as z:
                entry, = [n for n in z.namelist() if n.startswith(resource) and n.endswith('.so')]
                (native / output).write_bytes(z.read(entry))
        for binary in [java / 'bin/java', *java.rglob('*.so'), *native.glob('*.so')]:
            result = subprocess.run(['ldd', str(binary)], text=True, capture_output=True,
                env={**os.environ, 'LD_LIBRARY_PATH': str(java / 'lib') + ':' + str(java / 'lib/server')})
            if 'not found' in result.stdout:
                raise RuntimeError(str(binary) + '\n' + result.stdout)
            for entry in re.findall(r'(?<!\S)(/[^\s]+)', result.stdout):
                source = Path(entry)
                if source.is_relative_to(root):
                    continue
                target = root / source.relative_to('/')
                target.parent.mkdir(parents=True, exist_ok=True)
                if not target.exists():
                    shutil.copy2(source, target, follow_symlinks=True)
        (root / 'tmp').mkdir(mode=0o1777)
        (root / 'etc').mkdir()
        (root / 'etc/passwd').write_text('root:x:0:0:root:/tmp:/nonexistent\n')
        (root / 'etc/group').write_text('root:x:0:\n')
        (tmp / 'Dockerfile').write_text('''FROM scratch
COPY rootfs/ /
ENV JAVA_HOME=/opt/java
ENV SPARK_LOCAL_IP=127.0.0.1
ENV HOME=/tmp
ENV JAVA_TOOL_OPTIONS="-Xmx640m -XX:ActiveProcessorCount=2 -Djava.awt.headless=true -Duser.home=/tmp -Duser.timezone=UTC -Dorg.xerial.snappy.lib.path=/opt/native -DZstdNativePath=/opt/native/libzstd-jni.so --add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
''')
        subprocess.run(['sudo', '-n', '-u', 'cnic', '-g', 'docker', 'docker', 'build', '-t', 'dacp/operator-ecoflow-spark:1.0', str(tmp)], check=True)


if __name__ == '__main__':
    main()

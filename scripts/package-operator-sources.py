#!/usr/bin/env python3
"""Package current first-party sources and build definitions for publisher review."""
from pathlib import Path
import subprocess
import zipfile

root = Path(__file__).resolve().parents[1]
destination = root / '.operator-sources/dacp-sources.zip'
destination.parent.mkdir(exist_ok=True)
paths = subprocess.check_output(
    ['git', 'ls-files', '--cached', '--others', '--exclude-standard', '-z'], cwd=root
).decode().split('\0')
with zipfile.ZipFile(destination, 'w', zipfile.ZIP_DEFLATED) as archive:
    for name in sorted(set(paths)):
        path = root / name
        if name and path.is_file() and not path.is_symlink():
            entry = zipfile.ZipInfo(name, (1980, 1, 1, 0, 0, 0))
            entry.compress_type = zipfile.ZIP_DEFLATED
            archive.writestr(entry, path.read_bytes())
print(destination)

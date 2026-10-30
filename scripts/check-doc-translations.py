#!/usr/bin/env python3
"""Check bilingual Markdown pairing, language presence and local Markdown links."""
from pathlib import Path
import re
import subprocess
import sys

root = Path(__file__).resolve().parents[1]
paths = subprocess.check_output([
    'rg', '--files', '--hidden', '-g', '*.md', '-g', '!**/target/**',
    '-g', '!**/.git/**', '-g', '!**/.venv/**', '-g', '!**/node_modules/**',
    '-g', '!**/.union-demo/**', '-g', '!**/.bsp/**',
], cwd=root, text=True).splitlines()
errors = []
for relative in paths:
    path = root / relative
    if path.stem.endswith('_en'):
        errors.append(f'{relative}: English aliases are not allowed; use the unsuffixed file')
pairs = 0
for relative in sorted(paths):
    path = root / relative
    # Agent instructions are tooling configuration, not published documentation.
    if path.name in ('AGENTS.md', 'README_en.md'):
        continue
    chinese = path.stem.endswith('_zh')
    mate = path.with_name(path.stem[:-3] + '.md' if chinese else path.stem + '_zh.md')
    if not mate.is_file():
        errors.append(f'{relative}: missing {mate.name}')
    elif not chinese:
        pairs += 1
        if not re.search(r'[\u4e00-\u9fff]', mate.read_text()):
            errors.append(f'{mate.relative_to(root)}: Chinese text missing')
        if not re.search(r'[A-Za-z]{3,}', path.read_text()):
            errors.append(f'{relative}: English text missing')
for relative in paths:
    path = root / relative
    if path.name in ('AGENTS.md', 'README_en.md'):
        continue
    prose = re.sub(r'```.*?```', '', path.read_text(), flags=re.S)
    for link in re.findall(r'\]\(([^)]+)\)', prose):
        if '://' in link or link.startswith(('mailto:', 'app:')):
            continue
        target, _, anchor = link.partition('#')
        destination = path.parent / target if target else path
        if not destination.exists():
            errors.append(f'{relative}: broken local link {link}')
        elif anchor and destination.suffix == '.md':
            body = re.sub(r'```.*?```', '', destination.read_text(), flags=re.S)
            headings = re.findall(r'^#{1,6}\s+(.+)$', body, flags=re.M)
            slugs = [re.sub(r'[^\w\-\u4e00-\u9fff ]', '', h.lower()).replace(' ', '-') for h in headings]
            if anchor not in slugs:
                errors.append(f'{relative}: missing heading {link}')
for error in errors:
    print(error, file=sys.stderr)
print(f'{pairs} bilingual documentation pairs; {len(errors)} errors')
sys.exit(bool(errors))

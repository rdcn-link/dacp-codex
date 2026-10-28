#!/usr/bin/env python3
"""Build 100 operators sharing one wheel; optionally publish via authenticated HTTP."""
import argparse
import base64
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import shutil
import sys
import urllib.request
import zipfile
from catalog import CATALOG, parameter_schema

ROOT=Path(__file__).resolve().parents[3]
HERE=Path(__file__).resolve().parent

def archive(files):
    out=io.BytesIO()
    with zipfile.ZipFile(out,'w',zipfile.ZIP_DEFLATED) as z:
        for name,data in sorted(files.items()):
            info=zipfile.ZipInfo(name,(2026,1,1,0,0,0));info.compress_type=zipfile.ZIP_DEFLATED
            z.writestr(info,data)
    return out.getvalue()

def build(runtime_catalog,destination,version="1.1"):
    if not __import__("re").fullmatch(r"[0-9]+(?:\.[0-9]+)*",version):raise ValueError("version must be numeric dot-separated components")
    base=json.loads((runtime_catalog/'identity_python/1.1/manifest.json').read_text())
    files={'dacp_common/__init__.py':b'', 'dacp_common/catalog.py':(HERE/'catalog.py').read_bytes(),
           'dacp_operator/__init__.py':(ROOT/'dacp-operator-sdk/python/dacp_operator/__init__.py').read_bytes()}
    files['dacp_operator/inference.py'] = (ROOT / 'dacp-operator-sdk/python/dacp_operator/inference.py').read_bytes()
    for name in CATALOG:
        files[f'dacp_common/{name}.py']=(f'from dacp_operator import run_operator, preview_process\nfrom .catalog import execute, infer_outputs as infer\ndef process(ctx):\n    execute({name!r}, ctx)\ndef infer_outputs(inputs, params):\n    return infer({name!r}, inputs, params)\ndef preview_outputs(inputs, params, limits):\n    return preview_process(process, inputs, params, limits)\nif __name__ == "__main__":\n    run_operator(process, infer_outputs, preview_outputs=preview_outputs)\n').encode()
    dist=f'dacp_common_operators-{version}.dist-info/'
    files[dist+'METADATA']=f'Metadata-Version: 2.1\nName: dacp-common-operators\nVersion: {version}\nRequires-Python: >=3.10\nRequires-Dist: pyarrow>=23.0.1\n'.encode()
    files[dist+'WHEEL']=b'Wheel-Version: 1.0\nGenerator: dacp\nRoot-Is-Purelib: true\nTag: py3-none-any\n'
    records=[f'{n},sha256={base64.urlsafe_b64encode(hashlib.sha256(b).digest()).rstrip(b"=").decode()},{len(b)}' for n,b in sorted(files.items())]
    files[dist+'RECORD']=('\n'.join(records+[dist+'RECORD,,'])+'\n').encode()
    wheel=archive(files)
    sources=archive({'catalog.py':(HERE/'catalog.py').read_bytes(),'build.py':Path(__file__).read_bytes(),
                     'README.md':(HERE/'README.md').read_bytes(),'README_zh.md':(HERE/'README_zh.md').read_bytes(),
                     'dacp_operator/__init__.py':files['dacp_operator/__init__.py'],
                     **{n:b for n,b in files.items() if n.startswith('dacp_common/')}})
    blobs=destination/'blobs/sha256';blobs.mkdir(parents=True,exist_ok=True)
    def store(data,filename):
        sha=hashlib.sha256(data).hexdigest();(blobs/sha).write_bytes(data)
        return dict(sha256=sha,size=len(data),filename=filename)
    artifact=store(wheel,f'dacp_common_operators-{version}-py3-none-any.whl')
    source=store(sources,'dacp-common-operators-sources.zip')
    runtime=base['runtime'];src=runtime_catalog/'blobs/sha256'/runtime['archive']['sha256'];dst=blobs/src.name
    if not dst.exists():
        try:os.link(src,dst)
        except OSError:shutil.copyfile(src,dst)
    assert hashlib.sha256(dst.read_bytes()).hexdigest()==runtime['archive']['sha256']
    for name,s in CATALOG.items():
        desc=s['description']+'; params='+json.dumps(s['params'],ensure_ascii=False,separators=(',',':'))
        m=dict(api_version=2,icon=dict(path='M3 5h18v14H3zM3 10h18M9 5v14',color='#14866b'),name=name,version=version,description=desc,parameters=parameter_schema(s['params']),
               inputs={'source':{'representations':[s['representation']],'formats':[]}},
               execution={'executor':'python','entrypoint':'dacp_common.'+name,'config':{}},runtime=runtime,artifact=artifact,
               provenance={'source_archive':source,'source_repository':'https://github.com/rdcn-link/dacp-codex',
                           'build_instructions':'Run .venv/bin/python examples/operators/common/build.py --runtime-catalog .operator-catalog; see bundled bilingual README files. Requires the shared dacp/operator-python:1.0 runtime; no new image per operator.'})
        p=destination/name/version;p.mkdir(parents=True,exist_ok=True);(p/'manifest.json').write_text(json.dumps(m,ensure_ascii=False))
    return artifact,source

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--runtime-catalog',type=Path,default=ROOT/'.operator-catalog');p.add_argument('--output',type=Path,default=HERE/'target/catalog');p.add_argument('--version',default='1.1');a=p.parse_args()
    artifact,source=build(a.runtime_catalog,a.output,a.version)
    print(f'Built {len(CATALOG)} operators; shared wheel {artifact["size"]} bytes; sources {source["size"]} bytes')

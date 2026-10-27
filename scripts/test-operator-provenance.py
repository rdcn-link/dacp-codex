#!/usr/bin/env python3
import yaml
"""Integration: run a repository on localhost:8820, seed source-bearing examples, then run with Docker access."""
import json, socket, subprocess, tempfile, time, urllib.request
from pathlib import Path
from dacp import Job, Source
from security_test_support import authorize
ROOT=Path(__file__).resolve().parents[1]
creds={name:(ROOT/'examples/certs'/file).read_bytes() for name,file in [('tls_root_certs','ca.crt'),('cert_chain','client.crt'),('private_key','client.key')]}
with tempfile.TemporaryDirectory(prefix='dacp-repository-example-check-') as temp:
 base=Path(temp)
 with socket.socket() as s:
  s.bind(('127.0.0.1',0));port=s.getsockname()[1]
 node=f'dacp://localhost:{port}'
 payload=b'hello repository native and python\n'*40000
 (base/'input.txt').write_bytes(payload)
 (base/'table.csv').write_text('id,amount\n1,12.5\n2,30.0\n')
 config=dict(host='127.0.0.1',public_host='localhost',port=port,
  datasets=dict(data=dict(provider='mixed',items={'/input':dict(file=str(base/'input.txt')),'/table':dict(file=str(base/'table.csv'))})),
  local_operator_library=str(base/'no-local-operators'),
  remote_operator_library_urls=['http://localhost:8820'],operator_cache=str(base/'cache'),
  tls_cert=str(ROOT/'examples/certs/server.crt'),tls_key=str(ROOT/'examples/certs/server.key'),tls_ca=str(ROOT/'examples/certs/ca.crt'))
 creds['token']=authorize(config,base)
 (base/'config.json').write_text(json.dumps(config))
 with (base/'server.log').open('w+') as log:
  server=subprocess.Popen([str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'),str(base/'config.json')],cwd=ROOT/'dacp-server',stdin=subprocess.DEVNULL,stdout=log,stderr=log)
  try:
   for _ in range(150):
    if server.poll() is not None: raise RuntimeError('test server exited')
    try:
     with Source.open(node+'/data',**creds) as ds: ds.list()
     break
    except Exception: time.sleep(.1)
   else: raise RuntimeError('server readiness timeout')
   def run(operator,item='input',view='bytes',params=None):
    job=dict(name='repository-example-check',tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=node+'/data/'+item,view=view)),dict(id='check',operator=operator,inputs=dict(source='read'),params=params or {})],outputs={'result':'check'})
    with Job.run(node,yaml.safe_dump(job,sort_keys=False),timeout=60,**creds) as stream: events=list(stream)
    assert events[-1]['type']=='complete'
    return events
   events=run('identity@1.3')
   assert b''.join(e['data'] for e in events if e['type']=='data')==payload
   import pyarrow as pa, pyarrow.parquet as pq
   events=run('format_convert@1.3','table',params={'from':'csv','to':'parquet'})
   table=pq.read_table(pa.BufferReader(b''.join(e['data'] for e in events if e['type']=='data')))
   assert table.num_rows==2 and table.column_names==['id','amount']
   events=run('identity_python@1.1')
   assert b''.join(e['data'] for e in events if e['type']=='data')==payload
   events=run('identity_python@1.1','table','records')
   batches=[e['batch'] for e in events if e['type']=='data']
   assert sum(b.num_rows for b in batches)==2 and batches[0].schema.names==['id','amount']
   events=run('native_uppercase@1.1')
   assert b''.join(e['data'] for e in events if e['type']=='data')==payload.upper()
   assert len(list((base/'cache/blobs/sha256').glob('*')))>0
   for name in ['identity_python','native_uppercase']:
    with urllib.request.urlopen(f'http://localhost:8820/v1/operators/{name}/1.1') as r: manifest=json.load(r)
    with urllib.request.urlopen('http://localhost:8820/v1/blobs/sha256/'+manifest['artifact']['sha256']) as r:
     assert len(r.read())==manifest['artifact']['size']
    evidence=manifest['provenance']['source_archive']
    with urllib.request.urlopen('http://localhost:8820/v1/blobs/sha256/'+evidence['sha256']) as r:
     import hashlib
     data=r.read()
     assert len(data)==evidence['size'] and hashlib.sha256(data).hexdigest()==evidence['sha256']
    assert not (base/'cache/blobs/sha256'/evidence['sha256']).exists()
    with urllib.request.urlopen(f'http://localhost:8820/operators/{name}/1.1') as r:
     html=r.read().decode()
     assert 'Source archive' in html and 'License not declared' in html
   print('PASS: JVM identity (>1 MiB), JVM Parquet conversion, Python bytes/records, native uppercase (>1 MiB), evidence checksums and pages, no automatic evidence downloads.')
  except BaseException:
   log.flush();log.seek(0);print(log.read());raise
  finally:
   server.terminate()
   try:server.wait(timeout=15)
   except subprocess.TimeoutExpired:server.kill();server.wait()

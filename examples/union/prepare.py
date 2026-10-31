"""Prepare the two-node demo after staging both services and generating demo certificates."""
import hashlib
import json
import os
from pathlib import Path
import ssl
import subprocess

ROOT = Path(__file__).resolve().parents[2]
STATE = ROOT / '.union-demo'
NODE = ROOT / 'dacp-server/target/universal/stage/bin/dacp-server'
UNION_ADMIN = ROOT / 'dacp-union-server/target/universal/stage/bin/union-admin'
ADMIN = ROOT / 'dacp-server/target/universal/stage/bin/dacp-admin'
STATE.mkdir(exist_ok=True)

def local(program, *args):
    result = subprocess.run([str(program), *map(str, args)], cwd=ROOT, capture_output=True, text=True, check=True)
    return json.loads(result.stdout)

def secret_file(path, secret):
    path.write_text(secret + '\n')
    path.chmod(0o600)

config = dict(host='127.0.0.1', port=19860, public_url='dacp://localhost:19860',
    tls_cert=str(ROOT/'examples/certs/server.crt'), tls_key=str(ROOT/'examples/certs/server.key'),
    tls_ca=str(ROOT/'examples/certs/ca.crt'), state_directory=str(STATE/'x'),
    credential_directory=str(STATE/'x/credentials'), poll_interval_ms=500,
    execution_timeout_seconds=120, retention_seconds=60, web=dict(host='127.0.0.1', port=19880),
    members=[dict(url=f'dacp://localhost:{port}', credential_file=str(STATE/f'{name}-union.token')) for name,port in [('a',18861),('b',18862)]])
path=STATE/'x.yaml'
path.write_text(json.dumps(config, indent=2)+'\n')
identity=local(UNION_ADMIN, 'identity', path)
for name in ('a','b'):
    store=STATE/f'{name}-credentials'
    issued=local(ADMIN,'grant-union',store,'--subject','demo-union',
        '--union-id',identity['union_id'],'--fingerprint',identity['scheduler_identity'])
    secret_file(STATE/f'{name}-union.token',issued['token'])
issued=local(UNION_ADMIN,'create-account',STATE/'x/credentials','--subject','demo-user','--role','runner')
secret_file(STATE/'demo-user-account.json',json.dumps(issued,indent=2))
secret_file(STATE/'demo-user.token',issued['token'])
print('Prepared .union-demo/x.yaml and protected credential files. No secrets printed.')

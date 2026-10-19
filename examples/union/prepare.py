"""Run at repository root after scripts/certs.sh; prepares local demo configuration."""
import hashlib
import json
from pathlib import Path
import ssl
import time

root = Path(__file__).resolve().parents[2]
state = root/'.union-demo'
state.mkdir(exist_ok=True)
def identity(name):
    pem = (root/f'examples/certs/{name}.crt').read_text()
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert(pem)).hexdigest()

config = f'''host: 127.0.0.1
port: 19860
public_url: dacp://localhost:19860
tls_cert: ../examples/certs/server.crt
tls_key: ../examples/certs/server.key
tls_ca: ../examples/certs/ca.crt
state_directory: ../.union-demo/x
client_identities: [{identity('client')}]
poll_interval_ms: 500
execution_timeout_seconds: 120
retention_seconds: 60
web:
  host: 127.0.0.1
  port: 19880
'''
(state/'x.yaml').write_text(config)
for node, operator, port in [('a', 'quantity_sum@1.0', 18861), ('b', 'revenue_join@1.0', 18862)]:
    policy = dict(scheduler_identity=identity('server'), input_prefixes=[f'dacp://localhost:{port}/data'], operators=[operator], max_concurrent_tasks=4, expires_at_ms=int((time.time()+86400)*1000))
    # JSON is valid YAML and parsed by the same strict policy loader.
    (state/f'{node}-policy.yaml').write_text(json.dumps(policy, indent=2)+'\n')
print('Prepared .union-demo/x.yaml and per-node approval policies (valid for 24 hours).')

#!/usr/bin/env python3
"""Verify remote resolution and a real three-step Flight job.

Start a test node on localhost:18911 with weather/observations pointing to the
CSV fixture station_id,temperature / A,12.5 / B,13.0, max_concurrent_tasks>=3,
empty local operator library and remote_operator_library_urls=[http://localhost:8820].
Use examples/certs/node-a.{crt,key}, ca.crt for the node TLS configuration.
"""
import json
from pathlib import Path
import sys
root=Path(__file__).resolve().parents[3];sys.path.insert(0,str(root/'dacp-client'))
from dacp import Job
job=dict(job='piflow-remote-repository-contract',tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri='dacp://localhost:18911/weather/observations',view='bytes')),dict(id='read_csv',operator='piflow_csv_parser@1.0',inputs=dict(source='read')),dict(id='rename',operator='piflow_convert_schema@1.0',params=dict(schema='temperature->temperature_c'),inputs=dict(source='read_csv')),dict(id='export',operator='piflow_csv_save@1.0',inputs=dict(source='rename'))],outputs=dict(result='export'))
cert=root/'examples/certs'
with Job.run('dacp://localhost:18911',json.dumps(job),tls_root_certs=(cert/'ca.crt').read_bytes(),cert_chain=(cert/'client.crt').read_bytes(),private_key=(cert/'client.key').read_bytes(),timeout=180) as stream:
    result=b''.join(e['data'] for e in stream if e['type']=='data')
assert result==b'station_id,temperature_c\nA,12.5\nB,13.0\n',repr(result)
print('PASS remote HTTP operator resolution + Flight job + FIFO record streams + CSV output')
print(result.decode())

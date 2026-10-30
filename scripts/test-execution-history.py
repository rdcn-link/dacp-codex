#!/usr/bin/env python3
import yaml
"""Real Flight/Docker check for ordinary DACP job/task execution history."""
import json
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from dacp import Job
from security_test_support import authorize

ROOT = Path(__file__).resolve().parents[1]
NODE = ROOT/'dacp-server/target/universal/stage/bin/dacp-server'
CREDS = {name: (ROOT/'examples/certs'/file).read_bytes() for name, file in
         [('tls_root_certs','ca.crt'),('cert_chain','client.crt'),('private_key','client.key')]}


def main():
    with tempfile.TemporaryDirectory(prefix='dacp-history-smoke-') as temp:
        state = Path(temp)
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        origin = f'dacp://localhost:{port}'
        config = dict(host='127.0.0.1', public_host='localhost', port=port,
            datasets={'data': {'provider':'mixed','items':{'/input': {'file': str(ROOT/'examples/union/data/east_observations.csv')}}}},
            tls_cert=str(ROOT/'examples/certs/node-a.crt'),
            tls_key=str(ROOT/'examples/certs/node-a.key'), tls_ca=str(ROOT/'examples/certs/ca.crt'),
            mode='DEBUG',source_runtimes={'python@1.0': {'executor':'python','image':'xstream/operator-python:1.0'}},
            max_concurrent_tasks=4,execution_store=dict(provider='file',settings=dict(directory=str(state/'executions'))))
        CREDS['token'] = authorize(config, state)
        path=state/'server.yaml';path.write_text(json.dumps(config))
        with (state/'server.log').open('w+') as log:
            process=subprocess.Popen([str(NODE),str(path)],cwd=ROOT/'dacp-server',stdout=log,stderr=subprocess.STDOUT)
            try:
                deadline=time.monotonic()+30
                while True:
                    assert process.poll() is None
                    try:
                        with socket.create_connection(('localhost',port),timeout=1):break
                    except OSError:
                        assert time.monotonic()<deadline
                        time.sleep(.1)
                def run(code):
                    job=yaml.safe_dump(dict(name='history-test',tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=origin+'/data/input',view='bytes')),dict(id='run',operator=dict(runtime='python@1.0',source=code+"\ndef infer_outputs(inputs, params):\n    return {'outputs': {'result': inputs['source']}, 'diagnostics': []}\n"),inputs={'source':'read'},params={'output_format':'csv'})],outputs={'result':'run'}))
                    return Job.run(origin,job,timeout=30,**CREDS)
                with run("def process(inputs, params):\n    yield from inputs['source']\n") as stream:
                    assert list(stream)[-1]['type']=='complete'
                try:
                    with run('invalid Python syntax !') as stream:list(stream)
                except Exception:pass
                else:raise AssertionError('invalid source succeeded')
                with run("import time\ndef process(inputs, params):\n    yield b'first'\n    time.sleep(15)\n    yield from inputs['source']\n") as stream:
                    first=next(stream)
                    while first['type']=='execution': first=next(stream)
                    assert first['type']=='start'
                    assert next(stream)['data']==b'first'
                deadline=time.monotonic()+15
                while True:
                    records=[json.loads(p.read_text()) for p in (state/'executions').glob('*.json')]
                    if len(records)==7 and all(r['state'] in ('succeeded','failed','cancelled','expired') for r in records):break
                    assert time.monotonic()<deadline, records
                    time.sleep(.1)
                assert {r['state'] for r in records} >= {'succeeded','failed','cancelled'}
                workers=[r for r in records if r.get('request_type')=='task']
                assert len(workers)==4 and all(r.get('parent_execution_id') for r in workers)
                assert all('def process' not in json.dumps(r) for r in workers)
                listing=subprocess.run([str(NODE),'history','list',str(state/'executions')],capture_output=True,text=True,check=True)
                assert len(json.loads(listing.stdout))==7
                print('PASS: ordinary Flight jobs and worker tasks persist success, failure, cancellation, timing, correlation and live CLI history')
            except BaseException:
                log.flush();log.seek(0);print(log.read()[-15000:]);raise
            finally:
                process.terminate()
                try:process.wait(timeout=15)
                except subprocess.TimeoutExpired:process.kill();process.wait()

if __name__=='__main__':main()

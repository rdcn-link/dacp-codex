#!/usr/bin/env python3
"""Real Flight jobs: shared JVM/Python containers, preserved history, no live-server changes."""
import argparse
from datetime import datetime,timezone
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import time
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'dacp-client'))
from dacp.jobs import Job
from security_test_support import authorize
PYTHON='''from dacp_operator import OperatorContext
def process(ctx):
    out=ctx.bytes_output('result','text')
    for b in ctx.input('source').bytes():out.write(b.upper() if ctx.params.get('upper') else b)
def infer_outputs(inputs, params):
    raise AssertionError("execution must not invoke structure inference")
if __name__=='__main__':
    from dacp_operator.inference import run_operator
    run_operator(process, infer_outputs)
'''
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--docker-prefix',default='');a=p.parse_args();prefix=shlex.split(a.docker_prefix)
    with socket.socket() as s:s.bind(('127.0.0.1',0));port=s.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix='dacp-group-flight-') as tmp:
        d=Path(tmp);origin=f'dacp://localhost:{port}';(d/'message.txt').write_text('hello dacp\n');(d/'ops.py').write_text(PYTHON)
        jar=ROOT/'examples/operators/common-scala/target/scala-2.13/dacp-common-scala_2.13-1.0.jar'
        if not jar.exists():raise RuntimeError('build the common Scala example JAR first')
        for name,executor,image,entry,artifact in [('upper','jvm','dacp/operator-jvm:1.1','dacp.common.Op_text_uppercase',jar),('strip','jvm','dacp/operator-jvm:1.1','dacp.common.Op_text_strip',jar),('python','python','dacp/operator-python:1.0','ops',d/'ops.py'),('legacy','python','dacp/operator-python:1.0','identity',ROOT/'examples/operators/python/identity_python/1.1/identity.py')]:
            target=d/'operators'/name/'1';target.mkdir(parents=True)
            (target/'operator.yaml').write_text(json.dumps(dict(name=name,version='1',image=image,executor=executor,entrypoint=entry,artifact=str(artifact),inputs={'source':dict(representations=['bytes'],formats=[])})))
        cert=ROOT/'examples/certs'
        config=dict(host='127.0.0.1',public_host='localhost',port=port,datasets={'data':dict(provider='mixed',items={'/message':dict(file=str(d/'message.txt'))})},tls_cert=str(cert/'node-a.crt'),tls_key=str(cert/'node-a.key'),tls_ca=str(cert/'ca.crt'),local_operator_library=str(d/'operators'),max_concurrent_tasks=4,execution_store=dict(provider='file',settings=dict(directory=str(d/'history'))),access_log=dict(directory=str(d/'access')))
        # Access log settings are optional; use the server defaults without assuming a shape.
        config.pop('access_log')
        token = authorize(config, d)
        (d/'node.yaml').write_text(json.dumps(config))
        since=datetime.now(timezone.utc).isoformat()
        with (d/'server.log').open('w') as log:
            proc=subprocess.Popen(prefix+[str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'),str(d/'node.yaml')],cwd=d,stdout=log,stderr=subprocess.STDOUT)
        try:
            for _ in range(150):
                if proc.poll() is not None:raise RuntimeError((d/'server.log').read_text())
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.1):break
                except OSError:time.sleep(.1)
            else:raise RuntimeError('server startup timeout')
            tls=dict(token=token,tls_root_certs=(cert/'ca.crt').read_bytes(),cert_chain=(cert/'client.crt').read_bytes(),private_key=(cert/'client.key').read_bytes())
            for language,ops,expected in [('jvm',['upper@1','strip@1'],b'HELLO DACP'),('python',['python@1','python@1'],b'HELLO DACP\n'),('mixed',['upper@1','legacy@1'],b'HELLO DACP\n')]:
                tasks=[dict(id='a',operator=ops[0],inputs={'source':'read'},params={'upper':True} if language=='python' else {}),dict(id='b',operator=ops[1],inputs={'source':'a'},params={})]
                job=dict(job='fusion-'+language,tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=origin+'/data/message',view='bytes'))]+tasks,outputs={'result':'b'})
                started=time.monotonic()
                with Job.run(origin,json.dumps(job),timeout=180,**tls) as stream:
                    frames=list(stream)
                payload=b''.join(f['data'] for f in frames if f['type']=='data');assert payload==expected,(language,payload)
                print(language,'Flight job verified, elapsed',round(time.monotonic()-started,3),'s',flush=True)
            for _ in range(100):
                records=[json.loads(p.read_text()) for p in (d/'history').rglob('*.json')]
                if all(r['state']=='succeeded' for r in records):break
                time.sleep(.05)
            shared=[r for r in records if r.get('kind')=='dacp_exchange' and r.get('execution_mode')=='shared_process']
            assert len(shared)==2 and all(r['state']=='succeeded' for r in shared),records
            children=[r for r in records if r.get('kind')=='job_task'];assert len(children)==6 and all(r['state']=='succeeded' for r in children),records
            until=datetime.now(timezone.utc).isoformat()
            events=subprocess.run(prefix+['docker','events','--since',since,'--until',until,'--filter','type=container','--filter','event=create','--filter','label=dacp.execution_mode=shared_process','--format','{{json .}}'],capture_output=True,text=True,check=True)
            assert len(events.stdout.splitlines())==2,events.stdout
            isolated=[r for r in records if r.get('request_type')=='job' and r.get('execution_mode')=='isolated_steps']
            assert len(isolated)==1 and isolated[0]['fusion_reason']=='different_runtime_images',records
            print('Verified: four fused steps used exactly two containers; per-step history preserved; mixed environments used the original path')
        except BaseException:
            print((d/'server.log').read_text(),file=sys.stderr);raise
        finally:
            proc.terminate()
            try:proc.wait(timeout=15)
            except subprocess.TimeoutExpired:proc.kill();proc.wait()
if __name__=='__main__':main()

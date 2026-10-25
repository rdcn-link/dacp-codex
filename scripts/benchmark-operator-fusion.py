#!/usr/bin/env python3
"""Compare identical 3-stage Flight Jobs with fusion enabled/disabled; persist every sample."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import socket
import statistics
import subprocess
import sys
import time
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dacp-client'))
from dacp.jobs import Job
from security_test_support import authorize

PYTHON = '''from dacp_operator import OperatorContext
def process(ctx):
    out = ctx.bytes_output('result', 'text')
    upper = ctx.params['upper']
    for chunk in ctx.input('source').bytes():
        out.write(chunk.upper() if upper else chunk.lower())
def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    return result({'result': descriptor(['bytes'], ['text'], certainty='known')})
if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)
'''


def wheel(path):
    with zipfile.ZipFile(path, 'w', zipfile.ZIP_DEFLATED) as z:
        z.writestr('bench.py', PYTHON)
        for source in (ROOT / 'dacp-operator-sdk/python/dacp_operator').glob('*.py'):
            z.write(source, 'dacp_operator/' + source.name)
        z.writestr('dacp_bench-1.0.dist-info/METADATA', 'Metadata-Version: 2.1\nName: dacp-bench\nVersion: 1.0\n')
        z.writestr('dacp_bench-1.0.dist-info/WHEEL', 'Wheel-Version: 1.0\nGenerator: dacp-benchmark\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
        z.writestr('dacp_bench-1.0.dist-info/RECORD', '')


def dataset(path, rows):
    expected = hashlib.sha256()
    with path.open('wb') as f:
        for start in range(0, rows, 10000):
            chunk = b''.join((f'{i:010d},alpha,beta,gamma,delta,'.ljust(63, 'x') + '\n').encode() for i in range(start, min(start + 10000, rows)))
            f.write(chunk)
            expected.update(chunk.upper())
    return dict(rows=rows, bytes=path.stat().st_size, sha256=expected.hexdigest())


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker-prefix', default='')
    parser.add_argument('--rows', type=int, nargs='+', default=[1000000, 4000000])
    parser.add_argument('--repeats', type=int, default=3)
    parser.add_argument('--languages', nargs='+', choices=['jvm', 'python'], default=['jvm', 'python'])
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    assert args.repeats > 0 and all(n > 0 for n in args.rows)
    d = args.output.resolve()
    d.mkdir(parents=True, exist_ok=False)
    docker_prefix = shlex.split(args.docker_prefix)
    cert = ROOT / 'examples/certs'
    jar = ROOT / 'examples/operators/stream-benchmark/target/scala-2.13/dacp-stream-benchmark_2.13-0.1.0-SNAPSHOT.jar'
    if 'jvm' in args.languages and not jar.exists():
        raise RuntimeError('Run sbt package inside examples/operators/stream-benchmark first')
    bundle = d / 'dacp_bench-1.0-py3-none-any.whl'
    wheel(bundle)
    datasets = {}
    for rows in args.rows:
        datasets[str(rows)] = dataset(d / f'{rows}.txt', rows)
    print('datasets:', json.dumps(datasets), flush=True)
    for language, image, entry, artifact in [('jvm', 'dacp/operator-jvm:1.1', 'bench.Transform', jar), ('python', 'dacp/operator-python:1.0', 'bench', bundle)]:
        target = d / 'operators' / language / '1'
        target.mkdir(parents=True)
        (target / 'operator.yaml').write_text(json.dumps(dict(name=language, version='1', image=image, executor=language, entrypoint=entry, artifact=str(artifact), inputs={'source':dict(representations=['bytes'], formats=[])})))
    result = dict(started_at=datetime.now(timezone.utc).isoformat(), host=platform.platform(), cpu_count=os.cpu_count(), datasets=datasets, steps=3, repetitions=args.repeats, samples=[], warmups=[], methodology='Same code, image, params, data and client; server operator_fusion_enabled only differs. Alternating order; one warmup per language/mode; wall time includes Flight submission through complete result drain and client SHA-256/row validation. Each row is 64 bytes; CSV text is transported as a byte stream, not Arrow records.')
    result['images'] = json.loads(subprocess.check_output(docker_prefix + ['docker', 'image', 'inspect', 'dacp/operator-jvm:1.1', 'dacp/operator-python:1.0'], text=True))
    # Retain only reproducibility metadata, not the entire image configuration.
    result['images'] = [dict(id=i['Id'], tags=i['RepoTags']) for i in result['images']]
    result['git_head'] = subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip()
    result['working_tree_modified'] = bool(subprocess.check_output(['git', 'status', '--porcelain'], cwd=ROOT, text=True))
    processes = []
    nodes = {}
    tokens = {}
    def save():
        (d / 'results.json').write_text(json.dumps(result, indent=2))
    try:
        for mode in ['fused', 'isolated']:
            with socket.socket() as s:
                s.bind(('127.0.0.1', 0)); port = s.getsockname()[1]
            origin = f'dacp://localhost:{port}'
            work = d / mode
            work.mkdir()
            config = dict(host='127.0.0.1', public_host='localhost', port=port, datasets={'data':dict(provider='mixed', items={'/' + str(n):dict(file=str(d / f'{n}.txt')) for n in args.rows})}, tls_cert=str(cert / 'node-a.crt'), tls_key=str(cert / 'node-a.key'), tls_ca=str(cert / 'ca.crt'), local_operator_library=str(d / 'operators'), max_concurrent_tasks=4, operator_fusion_enabled=mode == 'fused', execution_store=dict(provider='file', settings=dict(directory=str(work / 'history'))))
            tokens[mode] = authorize(config, work)
            (work / 'node.yaml').write_text(json.dumps(config, indent=2))
            with (work / 'server.log').open('w') as log:
                p = subprocess.Popen(docker_prefix + [str(ROOT / 'dacp-server/target/universal/stage/bin/dacp-server'), str(work / 'node.yaml')], cwd=work, stdout=log, stderr=subprocess.STDOUT)
            processes.append(p)
            for _ in range(600):
                if p.poll() is not None:
                    raise RuntimeError((work / 'server.log').read_text())
                try:
                    with socket.create_connection(('127.0.0.1', port), timeout=.1): break
                except OSError: time.sleep(.1)
            else: raise RuntimeError('server start timeout')
            nodes[mode] = origin
        tls = dict(tls_root_certs=(cert / 'ca.crt').read_bytes(), cert_chain=(cert / 'client.crt').read_bytes(), private_key=(cert / 'client.key').read_bytes())
        sequence = 0
        def run(language, rows, mode, warmup, repeat):
            nonlocal sequence
            sequence += 1
            origin = nodes[mode]
            tasks = [dict(id=f'step{i}', operator=language + '@1', inputs={'source':'read' if i == 0 else f'step{i-1}'}, params={'upper':i % 2 == 0}) for i in range(3)]
            job = dict(job=f'benchmark-{sequence}', tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=origin+'/data/'+str(rows),view='bytes'))]+tasks, outputs={'result':'step2'})
            (d / f'job-{sequence}.json').write_text(json.dumps(job, indent=2))
            since = datetime.now(timezone.utc).isoformat()
            start = time.perf_counter()
            sha, size, count, first = hashlib.sha256(), 0, 0, None
            with Job.run(origin, json.dumps(job), timeout=900, token=tokens[mode], **tls) as stream:
                for event in stream:
                    if event['type'] == 'data':
                        if first is None: first = time.perf_counter() - start
                        data = event['data']; sha.update(data); size += len(data); count += data.count(b'\n')
            elapsed = time.perf_counter() - start
            assert size == datasets[str(rows)]['bytes'] and count == rows and sha.hexdigest() == datasets[str(rows)]['sha256'], (size, count, sha.hexdigest())
            # Wait for server final history write; client EOS precedes persistence by a few ms.
            for _ in range(200):
                history = [json.loads(p.read_text()) for p in (d / mode / 'history').rglob('*.json')]
                pending = [r for r in history if r.get('request_type') == 'job' and r.get('state') not in ['succeeded', 'failed', 'cancelled', 'expired']]
                if not pending: break
                time.sleep(.05)
            expected_mode = 'shared_process' if mode == 'fused' else 'isolated_steps'
            jobs = [r for r in history if r.get('request_type') == 'job']
            assert jobs and all(r.get('execution_mode') == expected_mode and r['state'] == 'succeeded' for r in jobs), jobs
            until = datetime.now(timezone.utc).isoformat()
            events = subprocess.check_output(docker_prefix + ['docker', 'events', '--since', since, '--until', until, '--filter', 'type=container', '--filter', 'event=create', '--filter', 'label=dacp.execution_mode', '--format', '{{json .}}'], text=True)
            events = [json.loads(line) for line in events.splitlines()]
            containers = [e for e in events if e.get('Actor', {}).get('Attributes', {}).get('dacp.task') in ['group','step0','step1','step2']]
            assert len(containers) == (1 if mode == 'fused' else 3), containers
            sample = dict(language=language, rows=rows, input_bytes=size, mode=mode, repeat=repeat, wall_seconds=elapsed, first_data_seconds=first, mib_per_second=size / 1048576 / elapsed, containers=len(containers), output_sha256=sha.hexdigest(), started_at=since)
            result['warmups' if warmup else 'samples'].append(sample); save()
            print(('warmup' if warmup else 'sample'), json.dumps(sample), flush=True)
        for language in args.languages:
            for mode in ['fused','isolated']:
                run(language, min(args.rows), mode, True, 0)
            for rows in args.rows:
                for repeat in range(1, args.repeats + 1):
                    for mode in (['fused','isolated'] if repeat % 2 else ['isolated','fused']):
                        run(language, rows, mode, False, repeat)
        result['summary'] = []
        for language in args.languages:
            for rows in args.rows:
                medians = {mode:statistics.median(s['wall_seconds'] for s in result['samples'] if s['language'] == language and s['rows'] == rows and s['mode'] == mode) for mode in ['fused','isolated']}
                result['summary'].append(dict(language=language, rows=rows, **medians, speedup=medians['isolated'] / medians['fused'], reduction_percent=100 * (1 - medians['fused'] / medians['isolated'])))
        result['completed_at'] = datetime.now(timezone.utc).isoformat(); save()
        print('SUMMARY', json.dumps(result['summary'], indent=2), flush=True)
    finally:
        for p in processes: p.terminate()
        for p in processes:
            try: p.wait(timeout=15)
            except subprocess.TimeoutExpired: p.kill(); p.wait()
        save()

if __name__ == '__main__': main()

#!/usr/bin/env python3
"""Live native-command contracts. Requires staged server, Docker, cc and Python runtime image.
Run from the root: sg docker -c '.venv/bin/python scripts/test-native-execution.py'
"""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import time
from dacp import Job, Source
from security_test_support import authorize

ROOT = Path(__file__).resolve().parents[1]
NODE = 'dacp://localhost:' + os.environ.get('DACP_NATIVE_TEST_PORT', '18817')
CREDS = {name: (ROOT/'examples/certs'/file).read_bytes() for name, file in
         [('tls_root_certs','ca.crt'),('cert_chain','client.crt'),('private_key','client.key')]}


def main():
    with tempfile.TemporaryDirectory(prefix='dacp-native-contract-') as temp:
        base = Path(temp)
        executable = base/'uppercase'
        subprocess.run(['cc', '-O2', '-Wall', '-Wextra', '-Werror', '-s', '-o', str(executable),
                        str(ROOT/'examples/operators/native/uppercase.c')], check=True)
        # Deliberately remove executable permissions: publication preserves bytes, not mode bits.
        executable.chmod(0o400)
        library = base/'operators'
        for name, args in [('uppercase', []), ('failure', ['--fail']), ('slow', ['--slow']), ('installed', [])]:
            definition = dict(name=name, version='1', image='dacp/operator-python:1.0', executor='command',
                              entrypoint='/operator/uppercase', artifact=str(executable),
                              executor_config=dict(args=args, stdin='source', stdout=dict(
                                  port='result', representation='bytes', format='text')),
                              inputs=dict(source=dict(representations=['bytes'], formats=[])))
            if name == 'installed':
                definition.pop('artifact')
                definition['entrypoint'] = '/usr/local/bin/python'
                definition['executor_config']['args'] = ['-u', '-c',
                    "import os\nwhile True:\n data=os.read(0,4096)\n if not data: break\n os.write(1,data.upper())"]
            path = library/name/'1'/'operator.yaml'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(definition))
        for name, code, inp, out in [
            ('generate', "import os; os.write(1,b'generated\\n')", None,
             dict(port='result', representation='bytes', format='text')),
            ('sink', 'import sys; sys.stdin.buffer.read()', 'source', None),
            ('prepare', 'pass', None, None),
        ]:
            path = library/name/'1'/'operator.yaml'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(dict(name=name, version='1', image='dacp/operator-python:1.0',
                executor='command', entrypoint='/usr/local/bin/python',
                executor_config=dict(args=['-u', '-c', code], stdin=inp, stdout=out),
                inputs={} if inp is None else {inp: dict(representations=['bytes'], formats=[])})))
        payload = bytes(range(256)) + b'hello native streams\n' * 65536  # Larger than pipes and bounded bridge buffers.
        (base/'source.txt').write_bytes(payload)
        (base/'empty.bin').write_bytes(b'')
        config = dict(host='127.0.0.1', public_host='localhost', port=int(NODE.rsplit(':', 1)[1]),
                      datasets=dict(data=dict(provider='mixed', items={'/source': dict(file=str(base/'source.txt')), '/empty': dict(file=str(base/'empty.bin'))})),
                      local_operator_library=str(library), max_concurrent_tasks=4,
                      tls_cert=str(ROOT/'examples/certs/server.crt'), tls_key=str(ROOT/'examples/certs/server.key'),
                      tls_ca=str(ROOT/'examples/certs/ca.crt'))
        CREDS['token'] = authorize(config, base)
        (base/'server.json').write_text(json.dumps(config))
        with open(base/'server.log', 'w+') as log:
            server = subprocess.Popen([str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'), str(base/'server.json')],
                                      cwd=ROOT/'dacp-server', stdout=log, stderr=log)
            def job(name, view='bytes', item='source'):
                return json.dumps(dict(name='native-contract', tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=NODE+'/data/'+item,view=view)),dict(id='native', operator=name+'@1',inputs=dict(source='read'))],outputs={'result':'native'}))
            def run(name, item='source'):
                with Job.run(NODE, job(name, item=item), timeout=30, **CREDS) as stream:
                    return list(stream)
            try:
                for _ in range(150):
                    if server.poll() is not None:
                        raise RuntimeError('test server exited')
                    try:
                        with Source.open(NODE+'/data', **CREDS) as ds:
                            ds.list()
                        break
                    except Exception:
                        time.sleep(.1)
                else:
                    raise RuntimeError('test server did not start')
                source_job = dict(name='native-source', tasks=[dict(id='generate', operator='generate@1', inputs={})], outputs={'result':'generate'})
                with Job.run(NODE, json.dumps(source_job), timeout=30, **CREDS) as stream:
                    generated = list(stream)
                assert b''.join(e['data'] for e in generated if e['type']=='data') == b'generated\n'
                sink_job = dict(name='native-stream-sink', tasks=[
                    dict(id='generate', operator='generate@1', inputs={}),
                    dict(id='sink', operator='sink@1', inputs={'source':'generate'})], outputs={})
                with Job.run(NODE, json.dumps(sink_job), timeout=30, **CREDS) as stream:
                    sink_frames = list(stream)
                assert sink_frames[-1]['type']=='complete' and not any(e['type']=='data' for e in sink_frames)
                result = run('uppercase')
                assert b''.join(e['data'] for e in result if e['type']=='data') == payload.upper()
                assert result[-1]['type']=='complete'
                assert b''.join(e['data'] for e in run('installed') if e['type']=='data') == payload.upper()
                empty = run('uppercase', item='empty')
                assert [e['type'] for e in empty if e['type'] != 'execution'] == ['start', 'end', 'complete']
                partial = []
                try:
                    with Job.run(NODE, job('failure'), timeout=30, **CREDS) as stream:
                        partial.extend(stream)
                except Exception:
                    assert any(e['type']=='data' for e in partial)
                    assert not any(e['type']=='complete' for e in partial)
                else:
                    raise AssertionError('nonzero command succeeded')
                start = time.monotonic()
                with Job.run(NODE, job('slow'), timeout=30, **CREDS) as stream:
                    first = next(stream)
                    while first['type']=='execution': first=next(stream)
                    assert first['type']=='start'
                    assert next(stream)['type']=='data'
                    assert time.monotonic()-start < 12
                # Give cancellation time to remove the container, then assert no bridge process remains.
                for _ in range(100):
                    running = subprocess.check_output(['docker','ps','--no-trunc','--format','{{.Command}}'], text=True)
                    if 'import json,os,shutil' not in running:
                        break
                    time.sleep(.1)
                else:
                    raise AssertionError('native container survived cancellation')
                assert run('uppercase')[-1]['type']=='complete'
                print('Native contracts passed: >1 MiB binary stream, empty input, runtime executable, non-executable artifact, partial failure, incremental output, cancellation, recovery.')
            except BaseException:
                log.flush(); log.seek(0); print(log.read())
                raise
            finally:
                server.terminate()
                try:
                    server.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    server.kill(); server.wait()


if __name__ == '__main__':
    main()

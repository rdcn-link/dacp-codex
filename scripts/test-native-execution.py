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
                              inputs=dict(source=dict(representations=['bytes'], formats=[])),
                              outputs=dict(result=dict(representations=['bytes'], formats=['text'])))
            if name == 'installed':
                definition.pop('artifact')
                definition['entrypoint'] = '/usr/local/bin/python'
                definition['executor_config']['args'] = ['-u', '-c',
                    "import os\nwhile True:\n data=os.read(0,4096)\n if not data: break\n os.write(1,data.upper())"]
            path = library/name/'1'/'operator.yaml'
            path.parent.mkdir(parents=True)
            path.write_text(json.dumps(definition))
        payload = bytes(range(256)) + b'hello native streams\n' * 65536  # Larger than pipes and bounded bridge buffers.
        (base/'source.txt').write_bytes(payload)
        (base/'empty.bin').write_bytes(b'')
        config = dict(host='127.0.0.1', public_host='localhost', port=int(NODE.rsplit(':', 1)[1]),
                      datasets=dict(data=dict(provider='custom', items={'/source': dict(file=str(base/'source.txt')), '/empty': dict(file=str(base/'empty.bin'))})),
                      allowed_uri_prefixes=[NODE+'/data'], local_operator_library=str(library),
                      tls_cert=str(ROOT/'examples/certs/server.crt'), tls_key=str(ROOT/'examples/certs/server.key'),
                      tls_ca=str(ROOT/'examples/certs/ca.crt'))
        (base/'server.json').write_text(json.dumps(config))
        with open(base/'server.log', 'w+') as log:
            server = subprocess.Popen([str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'), str(base/'server.json')],
                                      cwd=ROOT/'dacp-server', stdout=log, stderr=log)
            def job(name, view='bytes', item='source'):
                return json.dumps(dict(job='native-contract', tasks=[dict(id='native', operator=name+'@1',
                    inputs=dict(source=dict(uri=NODE+'/data/'+item, view=view)))]))
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
                result = run('uppercase')
                assert b''.join(e['data'] for e in result if e['type']=='data') == payload.upper()
                assert result[-1]['type']=='complete'
                assert b''.join(e['data'] for e in run('installed') if e['type']=='data') == payload.upper()
                empty = run('uppercase', item='empty')
                assert [e['type'] for e in empty] == ['start', 'end', 'complete']
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
                    assert next(stream)['type']=='start'
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

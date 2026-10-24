#!/usr/bin/env python3
"""Live mTLS/Docker contract check against examples/source-debug.yaml (port 18815)."""
import io
import json
from pathlib import Path
import time
import pyarrow as pa
from dacp import Job
from dacp.cli import Browser

ROOT = Path(__file__).resolve().parents[1]
CREDS = {name: (ROOT/'examples/certs'/file).read_bytes() for name, file in
         [('tls_root_certs','ca.crt'),('cert_chain','client.crt'),('private_key','client.key')]}
NODE = 'dacp://localhost:18815'


def job(code, runtime='python@1.0', view='bytes', entrypoint=None):
    operator = dict(runtime=runtime, source=code)
    if entrypoint:
        operator['entrypoint'] = entrypoint
    return json.dumps(dict(job='source-contract', tasks=[dict(id='read',operator='read_item@1.0',inputs={},params=dict(uri=NODE+'/data1/orders',view=view)),dict(id='run', operator=operator,inputs=dict(source='read'), params=dict(output_format='csv'))],outputs={'result':'run'}))


def events(text):
    with Job.run(NODE, text, timeout=60, **CREDS) as stream:
        return list(stream)


def main():
    expected = (ROOT/'examples/data/nested/orders.csv').read_bytes().upper()
    code = "def process(inputs, params):\n    for chunk in inputs['source']:\n        yield chunk.upper()\n"
    result = events(job(code))
    assert b''.join(e['data'] for e in result if e['type']=='data') == expected
    assert result[-1]['type'] == 'complete'
    result = events(job("def process(inputs, params):\n    yield from inputs['source']\n", view='records'))
    batches = [e['batch'] for e in result if e['type']=='data']
    assert pa.Table.from_batches(batches).num_rows == 2
    scala = (ROOT/'examples/operators/inline/Uppercase.scala').read_text()
    result = events(job(scala, runtime='scala@1.0', entrypoint='Uppercase'))
    assert b''.join(e['data'] for e in result if e['type']=='data') == expected
    try:
        events(job('this is not valid python !'))
    except Exception as error:
        assert 'SyntaxError' in str(error)
    else:
        raise AssertionError('invalid source succeeded')
    # First output arrives while the program remains alive; closing cancels the 15-second sleep.
    slow = "import time\ndef process(inputs, params):\n    yield b'first'\n    time.sleep(15)\n    yield from inputs['source']\n"
    start = time.monotonic()
    with Job.run(NODE, job(slow), timeout=30, **CREDS) as stream:
        assert next(stream)['type'] == 'start'
        assert next(stream)['data'] == b'first'
        assert time.monotonic()-start < 12
    assert time.monotonic()-start < 12
    output = io.StringIO()
    browser = Browser(NODE+'/data1', CREDS, stdout=output)
    browser.use_rawinput = False
    browser.stdin = io.StringIO(code+'STOP\n')
    try:
        browser.onecmd('run --runtime python@1.0 --input source=/orders --params \'{"output_format":"csv"}\' <<STOP')
        assert 'ID,AMOUNT' in output.getvalue() and 'Job complete.' in output.getvalue(), output.getvalue()
        failing = "def process(inputs, params):\n    yield b'partial'\n    raise ValueError('deliberate failure')\n"
        browser.stdin = io.StringIO(failing+'END\n')
        browser.onecmd('run --runtime python@1.0 --input source=/orders <<END')
        assert 'results above are partial' in output.getvalue()
        assert 'Error:' in output.getvalue()
    finally:
        browser.close()
    print('Source contracts passed: Python bytes/records, Scala compilation, syntax failure, incremental cancellation, heredoc shell, partial failure.')


if __name__ == '__main__':
    main()

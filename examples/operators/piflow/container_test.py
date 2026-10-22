#!/usr/bin/env python3
"""Run existing Python contract fixtures against the actual packaged runtime and wheel."""
import io
import json
import os
from pathlib import Path
import struct
import subprocess
import sys
import tempfile
import zipfile

import pyarrow as pa
import test as fixtures

HERE = Path(__file__).resolve().parent


def execute(name, params, inputs):
    catalog = Path(os.environ.get('DACP_PIFLOW_CATALOG', HERE / 'target/catalog'))
    specification = json.loads((HERE / 'specs.json').read_text())[name]
    inventory = json.loads((HERE / 'inventory.json').read_text())
    operator = next(i['operator'] for i in inventory['operators'] if i['upstream_class'].endswith('.' + specification['class']))
    manifest = json.loads((catalog / operator / '1.0/manifest.json').read_text())
    with tempfile.TemporaryDirectory(prefix='dacp-piflow-contract-') as tmp:
        directory = Path(tmp)
        directory.chmod(0o755)
        code = directory / 'code'
        code.mkdir()
        with zipfile.ZipFile(catalog / 'blobs/sha256' / manifest['artifact']['sha256']) as wheel:
            wheel.extractall(code)
        entries = []
        for port, table in inputs.items():
            representation = 'records'
            if isinstance(table, bytes):
                representation = 'bytes'
                table = pa.table({'data': pa.array([table[i:i+3] for i in range(0, len(table), 3)], type=pa.binary())})
            with (directory / (port + '.arrow')).open('wb') as f:
                with pa.ipc.new_stream(f, table.schema) as writer:
                    writer.write_table(table, max_chunksize=1)
            entries.append(dict(port=port, file=port + '.arrow', representation=representation, format='arrow'))
        (directory / 'manifest.json').write_text(json.dumps(entries))
        (directory / 'params.json').write_text(json.dumps(params))
        command = ['sudo', '-n', '-u', 'cnic', '-g', 'docker', 'docker', 'run', '--rm', '--network', 'none',
            '--read-only', '--memory', '1g', '--pids-limit', '128', '--tmpfs', '/tmp:rw,noexec,nosuid,nodev,size=512m',
            '-v', str(directory) + ':/input:ro', '-e', 'PYTHONPATH=/input/code:/opt/python/lib/python3.12/site-packages', '--entrypoint', 'python',
            manifest['runtime']['image'], '-m', manifest['execution']['entrypoint'], 'operator', '/input', '/input/params.json']
        result = subprocess.run(command, capture_output=True, timeout=120)
        if result.returncode:
            error = result.stderr.decode(errors='replace')[-8000:]
            if 'ValueError:' in error:
                raise ValueError(error)
            raise RuntimeError(error)
        data = result.stdout
        output, representations, ended = {}, {}, set()
        while data:
            h, n = struct.unpack('>II', data[:8])
            assert h <= 65536 and n <= 8*1024*1024 and len(data) >= 8+h+n
            header, payload = json.loads(data[8:8+h]), data[8+h:8+h+n]
            data = data[8+h+n:]
            port = header['port']
            if header['type'] == 'start':
                assert port not in output
                representations[port] = header['representation']
                output[port] = [] if header['representation'] == 'records' else b''
            elif header['type'] == 'data':
                assert port not in ended
                if representations[port] == 'records':
                    output[port].extend(pa.ipc.open_stream(payload).read_all().to_pylist())
                else:
                    output[port] += payload
            elif header['type'] == 'end':
                ended.add(port)
            else:
                raise AssertionError(header)
        assert set(output) == ended and ended
        return output


if __name__ == '__main__':
    fixtures.execute = execute
    for name in sys.argv[1:]:
        fixtures.verify(name)

#!/usr/bin/env python3
"""Exercise bundled operators' actual stdout stream boundary (Docker/PyArrow required)."""
import argparse
import json
from pathlib import Path
import struct
import subprocess
import tempfile
import pyarrow as pa
import pyarrow.parquet as pq


def decode(raw):
    offset, payload, ended = 0, bytearray(), False
    while offset < len(raw):
        header_size, size = struct.unpack_from('>II', raw, offset)
        offset += 8
        header = json.loads(raw[offset:offset+header_size])
        offset += header_size
        data = raw[offset:offset+size]
        offset += size
        assert len(data) == size
        if header['type'] == 'data': payload.extend(data)
        if header['type'] == 'end': ended = True
    assert offset == len(raw) and ended
    return bytes(payload)


def run(image, reference, source):
    result = subprocess.run(['docker','run','--rm','--network','none','--read-only','--tmpfs','/tmp',
        '-e',f'DACP_OPERATOR={reference}','-v',f'{source}:/input:ro',image,
        'operator','/input','/input/params.json'], check=True, stdout=subprocess.PIPE)
    return decode(result.stdout)


def check(image):
    with tempfile.TemporaryDirectory(prefix='dacp-operator-contract-') as temporary:
        source = Path(temporary)
        for format_name, payload in [('csv',b'id,value\n1,2.5\n2,3.5\n'),
                ('ndjson',b'{"id":1,"value":2.5}\n{"id":2,"value":3.5}\n')]:
            schema = pa.schema([('data',pa.binary())])
            with (source/'input.arrow').open('wb') as raw:
                with pa.ipc.new_stream(raw,schema) as writer:
                    for part in (payload[:7],payload[7:]):
                        writer.write_batch(pa.record_batch([pa.array([part])],schema=schema))
            (source/'manifest.json').write_text(json.dumps([dict(port='source',file='input.arrow',representation='bytes',format=format_name)]))
            for compression in ('none','snappy','zstd'):
                (source/'params.json').write_text(json.dumps(dict(to='parquet',compression=compression,row_group_size='1MB')))
                table = pq.read_table(pa.BufferReader(run(image,'format_convert@1.1',source)))
                assert table.to_pydict() == {'id':[1,2],'value':[2.5,3.5]}
                assert b'dacp.arrow.schema' in table.schema.metadata
            assert run(image,'identity@1.1',source) == payload
            print(f'{format_name}: conversion (none/snappy/zstd), identity passed')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--image',default='dacp/format-convert:1.1')
    check(parser.parse_args().image)

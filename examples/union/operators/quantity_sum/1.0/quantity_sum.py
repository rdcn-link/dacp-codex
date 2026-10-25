"""Demo aggregate. Arrow byte input is CSV; output is product -> total quantity JSON."""
import csv
import io
import json
from pathlib import Path
import struct
import sys
import pyarrow as pa


def emit(kind, data=b'', **metadata):
    h = json.dumps(dict(type=kind, port='result', **metadata)).encode()
    sys.stdout.buffer.write(struct.pack('>II', len(h), len(data)) + h + data)
    sys.stdout.buffer.flush()


def read_item(directory, item):
    if item['representation'] != 'bytes':
        raise ValueError('expected bytes input')
    data = bytearray()
    with (directory/item['file']).open('rb') as raw:
        with pa.ipc.open_stream(raw) as reader:
            if reader.schema != pa.schema([('data', pa.binary())]):
                raise ValueError('invalid bytes schema')
            for batch in reader:
                for value in batch.column(0):
                    data.extend(value.as_py())
                    if len(data) > 16*1024*1024:
                        raise ValueError('demo input exceeds 16 MiB')
    return data.decode()


def main():
    if len(sys.argv) != 4 or sys.argv[1] != 'operator':
        raise ValueError('expected operator INPUT PARAMS')
    directory = Path(sys.argv[2])
    if json.loads(Path(sys.argv[3]).read_text()):
        raise ValueError('no parameters supported')
    manifest = json.loads((directory/'manifest.json').read_text())
    if [m['port'] for m in manifest] != ['source']:
        raise ValueError('expected source input')
    totals = {}
    for row in csv.DictReader(io.StringIO(read_item(directory, manifest[0]))):
        quantity = int(row['quantity'])
        totals[row['product']] = totals.get(row['product'], 0) + quantity
    emit('start', representation='bytes', format='json')
    emit('data', json.dumps(totals, sort_keys=True).encode())
    emit('end')


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    return result({'result': descriptor(['bytes'], ['json'], certainty='known')})


if __name__ == '__main__':
    from dacp_operator import run_inference
    if len(sys.argv) > 1 and sys.argv[1] == 'infer':
        run_inference(infer_outputs)
    else:
        main()

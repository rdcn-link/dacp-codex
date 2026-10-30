"""Demo fusion. Combine remote quantities and local prices, return JSON revenues."""
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
    items = {m['port']: m for m in manifest}
    if set(items) != {'quantities', 'prices'}:
        raise ValueError('expected quantities and prices')
    from decimal import Decimal
    quantities = json.loads(read_item(directory, items['quantities']))
    prices = {r['product']: Decimal(r['price']) for r in csv.DictReader(io.StringIO(read_item(directory, items['prices'])))}
    result = {p: str(prices[p]*q) for p, q in quantities.items()}
    emit('start', representation='bytes', format='json')
    emit('data', (json.dumps(result, sort_keys=True)+'\n').encode())
    emit('end')


def infer_outputs(inputs, params):
    from xstream_operator import descriptor, result
    return result({'result': descriptor(['bytes'], ['json'], certainty='known')})


def preview_outputs(inputs, params, limits):
    raise ValueError('Use the equivalent context-based operator to preview bounded record samples')


if __name__ == '__main__':
    from xstream_operator import run_inference
    if len(sys.argv) > 1 and sys.argv[1] == 'infer':
        run_inference(infer_outputs, preview=preview_outputs)
    else:
        main()

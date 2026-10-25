"""Filter one records input and add a scaled_amount column; dependencies live in the runtime."""
import base64
import json
import math
from pathlib import Path
import struct
import sys

import pyarrow as pa
import pyarrow.compute as pc


OUTPUT_SCHEMA = pa.schema([
    ('id', pa.int64()),
    ('amount', pa.float64()),
    ('scaled_amount', pa.float64()),
])


def emit(kind, payload=b'', **metadata):
    header = json.dumps(dict(type=kind, port='result', **metadata)).encode('utf-8')
    if len(header) > 65536 or len(payload) > 8 * 1024 * 1024:
        raise ValueError('frame exceeds limits')
    sys.stdout.buffer.write(struct.pack('>II', len(header), len(payload)))
    sys.stdout.buffer.write(header)
    sys.stdout.buffer.write(payload)
    sys.stdout.buffer.flush()


def read_params(path):
    params = json.loads(path.read_text(encoding='utf-8'))
    if not isinstance(params, dict) or set(params) - {'min_amount', 'factor'}:
        raise ValueError('accepted parameters: min_amount, factor')
    result = {}
    for key, default in [('min_amount', 0.0), ('factor', 1.0)]:
        value = params.get(key, default)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(key + ' must be a finite number')
        result[key] = float(value)
    return result


def main():
    if len(sys.argv) != 4 or sys.argv[1] != 'operator':
        raise ValueError('expected operator INPUT PARAMS')
    directory = Path(sys.argv[2])
    params = read_params(Path(sys.argv[3]))
    manifest = json.loads((directory / 'manifest.json').read_text(encoding='utf-8'))
    if (len(manifest) != 1 or manifest[0]['port'] != 'source'
            or manifest[0]['representation'] != 'records'):
        raise ValueError('expected one records input named source')
    with (directory / manifest[0]['file']).open('rb') as raw:
        with pa.ipc.open_stream(raw) as reader:
            if (reader.schema.names.count('id') != 1 or reader.schema.names.count('amount') != 1
                    or reader.schema.field('id').type != pa.int64()
                    or reader.schema.field('amount').type != pa.float64()):
                raise ValueError('source requires id:int64 and amount:float64')
            emit('start', representation='records', format='arrow',
                 schema=base64.b64encode(OUTPUT_SCHEMA.serialize().to_pybytes()).decode('ascii'))
            for batch in reader:
                # Slice each input batch; serialize each output slice as one independent IPC stream.
                for offset in range(0, batch.num_rows, 512):
                    part = batch.slice(offset, 512)
                    selected = part.filter(pc.greater_equal(part.column('amount'), params['min_amount']))
                    if selected.num_rows == 0:
                        continue
                    output = pa.record_batch([
                        selected.column('id'), selected.column('amount'),
                        pc.multiply(selected.column('amount'), params['factor']),
                    ], schema=OUTPUT_SCHEMA)
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, OUTPUT_SCHEMA) as writer:
                        writer.write_batch(output)
                    emit('data', sink.getvalue().to_pybytes())
    emit('end')


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    if set(inputs) != {'source'} or set(params) - {'min_amount', 'factor'}:
        raise ValueError('expected source and min_amount/factor parameters')
    return result({'result': descriptor(['records'], ['arrow'], OUTPUT_SCHEMA, certainty='known')})


if __name__ == '__main__':
    from dacp_operator import run_inference
    if len(sys.argv) > 1 and sys.argv[1] == 'infer':
        run_inference(infer_outputs)
    else:
        main()

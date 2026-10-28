"""A named item stream operator; its dependencies belong in the shared runtime."""
import base64
import json
from pathlib import Path
import struct
import sys
import pyarrow as pa


def emit(kind, data=b'', **metadata):
    header = json.dumps(dict(type=kind, port='result', **metadata)).encode()
    if len(header) > 65536 or len(data) > 8*1024*1024:
        raise ValueError('frame exceeds limits')
    sys.stdout.buffer.write(struct.pack('>II', len(header), len(data)) + header + data)
    sys.stdout.buffer.flush()


def main():
    if len(sys.argv) != 4 or sys.argv[1] != 'operator':
        raise ValueError('expected operator INPUT PARAMS')
    source = Path(sys.argv[2])
    if json.loads(Path(sys.argv[3]).read_text()) != {}:
        raise ValueError('identity_python accepts no parameters')
    manifest = json.loads((source/'manifest.json').read_text())
    if len(manifest) != 1 or manifest[0]['port'] != 'source':
        raise ValueError('expected source input')
    item = manifest[0]
    with (source/item['file']).open('rb') as raw:
        with pa.ipc.open_stream(raw) as reader:
            metadata = dict(representation=item['representation'], format=item['format'])
            if item['representation'] == 'records':
                metadata['schema'] = base64.b64encode(reader.schema.serialize().to_pybytes()).decode()
            elif item['representation'] != 'bytes' or reader.schema != pa.schema([('data', pa.binary())]):
                raise ValueError('unsupported input representation')
            emit('start', **metadata)
            for batch in reader:
                if item['representation'] == 'bytes':
                    for value in batch.column(0):
                        data = value.as_py()
                        for offset in range(0, len(data), 65536):
                            emit('data', data[offset:offset+65536])
                else:
                    sink = pa.BufferOutputStream()
                    with pa.ipc.new_stream(sink, reader.schema) as writer:
                        writer.write_batch(batch)
                    emit('data', sink.getvalue().to_pybytes())
    emit('end')


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    if set(inputs) != {'source'} or params:
        raise ValueError('expected source and no parameters')
    return result({'result': inputs['source']})


def preview_outputs(inputs, params, limits):
    from dacp_operator import bounded_sample
    sample = inputs['source'].get('sample')
    return {'result': bounded_sample(sample, limits)} if sample is not None else {}


if __name__ == '__main__':
    from dacp_operator import run_inference
    if len(sys.argv) > 1 and sys.argv[1] == 'infer':
        run_inference(infer_outputs, preview=preview_outputs)
    else:
        main()

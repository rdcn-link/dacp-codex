"""Decode live_ticks JSONL incrementally into an explicit Arrow record schema."""
import json
import math
import pyarrow as pa

SCHEMA = pa.schema([('sequence', pa.int64()), ('timestamp', pa.string()),
                    ('station_id', pa.string()), ('temperature_c', pa.float64())])
MAX_LINE = 65536


def process(ctx):
    if ctx.params:
        raise ValueError('live_ticks_decode accepts no parameters')
    output = ctx.records_output('result', SCHEMA)
    pending = bytearray()

    def emit(line):
        row = json.loads(line)
        if not isinstance(row, dict) or set(row) != set(SCHEMA.names):
            raise ValueError('expected a live_ticks observation object')
        if type(row['sequence']) is not int or row['sequence'] < 1:
            raise ValueError('sequence must be a positive integer')
        if not all(isinstance(row[n], str) and row[n] for n in ('timestamp', 'station_id')):
            raise ValueError('timestamp and station_id must be nonempty strings')
        value = row['temperature_c']
        if type(value) not in (int, float) or not math.isfinite(value):
            raise ValueError('temperature_c must be finite')
        output.write(pa.RecordBatch.from_pylist([row], schema=SCHEMA))

    for chunk in ctx.input('source').bytes():
        # Chunk boundaries need not align with JSON lines or UTF-8 characters.
        for i, part in enumerate(chunk.split(b'\n')):
            if i:
                if pending.strip():
                    emit(pending)
                pending.clear()
            if len(pending) + len(part) > MAX_LINE:
                raise ValueError('JSONL line exceeds 64 KiB')
            pending.extend(part)
    if pending.strip():
        emit(pending)


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    return result({'result': descriptor(['records'], ['arrow'], SCHEMA, certainty='known')})


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

"""PiFlow Distinct: keep the first row per key across input batches."""
import pickle
from dacp_piflow_support import params, columns, indexes, MAX_BYTES


def process(ctx):
    p = params(ctx, optional=['columnNames'])
    source = ctx.input('source')
    names = columns(p.get('columnNames', ''), allow_empty=True) or source.schema.names
    indexes(source.schema, names)
    out = ctx.records_output('result', source.schema)
    seen, size = set(), 0
    for batch in source.records():
        keep = []
        for i, row in enumerate(batch.select(names).to_pylist()):
            key = pickle.dumps(row, protocol=4)
            if key not in seen:
                size += len(key) + 128
                if size > MAX_BYTES:
                    raise ValueError('distinct key state exceeds 128 MiB')
                seen.add(key)
                keep.append(i)
        if keep:
            out.write(batch.take(keep))


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

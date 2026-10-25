"""PiFlow Subtract: set difference over entire records (EXCEPT DISTINCT)."""
import pickle
import pyarrow as pa
from dacp_piflow_support import params, materialize, write_table


def process(ctx):
    params(ctx)
    left = materialize(ctx.input('left'))
    right = materialize(ctx.input('right'))
    if left.schema != right.schema:
        raise ValueError('subtract requires identical ordered schemas')
    excluded = {pickle.dumps(row) for row in right.to_pylist()}
    seen, keep = set(), []
    for i, row in enumerate(left.to_pylist()):
        key = pickle.dumps(row)
        if key not in excluded and key not in seen:
            seen.add(key)
            keep.append(i)
    write_table(ctx, left.take(pa.array(keep, type=pa.int64())))


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'right', 'left'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

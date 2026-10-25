"""PiFlow ColumnCheck: compare complete ordered column names, then pass left."""
from dacp_piflow_support import params, materialize, write_table


def process(ctx):
    params(ctx)
    left = materialize(ctx.input('left'))
    right = ctx.input('right')
    if left.schema.names != right.schema.names:
        raise ValueError('column names or order differ from the reference')
    for _ in right.records():
        pass
    write_table(ctx, left)


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

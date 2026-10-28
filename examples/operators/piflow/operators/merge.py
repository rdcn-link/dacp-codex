"""PiFlow Merge adapted to two explicit input ports and a streaming union."""
from dacp_piflow_support import params


def process(ctx):
    params(ctx)
    left = ctx.input('left')
    schema = left.schema
    out = ctx.records_output('result', schema)
    for batch in left.records():
        out.write(batch)
    right = ctx.input('right')
    if right.schema != schema:
        raise ValueError('merge requires identical ordered schemas')
    for batch in right.records():
        out.write(batch)


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'right', 'left'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    return result(outputs)


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

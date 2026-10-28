"""PiFlow Fork with two explicitly declared DACP output ports."""
from dacp_piflow_support import params


def process(ctx):
    params(ctx)
    source = ctx.input('source')
    left = ctx.records_output('left', source.schema)
    right = ctx.records_output('right', source.schema)
    for batch in source.records():
        left.write(batch)
        right.write(batch)


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'left': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}, 'right': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
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

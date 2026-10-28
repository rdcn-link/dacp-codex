"""PiFlow TableShow returns selected table data; presentation remains the client's job."""
from dacp_piflow_support import params, query


def process(ctx):
    p = params(ctx, optional=['showField'])
    fields = p.get('showField', '*')
    if not isinstance(fields, str) or not fields.strip():
        raise ValueError('showField must be a SQL projection string')
    query(ctx, 'SELECT ' + fields + ' FROM source')


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
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

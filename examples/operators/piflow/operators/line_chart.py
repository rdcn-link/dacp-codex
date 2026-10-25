"""PiFlow LineChart produces grouped chart data, without rendering an image."""
from dacp_piflow_support import params, aggregate


def process(ctx):
    p = params(ctx, ['abscissa', 'customizedProperties'])
    aggregate(ctx, p['abscissa'], p['customizedProperties'])


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

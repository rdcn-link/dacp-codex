"""PiFlow pie-chart grouped data adapted to Arrow records."""
from dacp_piflow_support import params, aggregate


def process(ctx):
    p = params(ctx, ['dimension', 'indicator', 'indicatorOption'])
    aggregate(ctx, p['dimension'], {p['indicator']: p['indicatorOption']})


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

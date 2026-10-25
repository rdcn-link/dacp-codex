"""PiFlow scatter-plot projection and ordering, without image rendering."""
from dacp_piflow_support import params, identifier, query


def process(ctx):
    p = params(ctx, ['legend', 'customizedProperties'])
    legend, dimensions = p['legend'], p['customizedProperties']
    if not isinstance(legend, str) or not legend or not isinstance(dimensions, dict) or not dimensions:
        raise ValueError('legend and nonempty dimension mapping required')
    if any(not isinstance(k, str) or not k or type(v) is not int for k, v in dimensions.items()):
        raise ValueError('dimension names must map to integers')
    if legend in dimensions or len(set(dimensions.values())) != len(dimensions):
        raise ValueError('unique dimensions and order positions required')
    names = [legend] + sorted(dimensions, key=dimensions.get)
    query(ctx, 'SELECT ' + ','.join(map(identifier, names)) + ' FROM source ORDER BY ' + identifier(legend) + ',' + identifier(names[1]))


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

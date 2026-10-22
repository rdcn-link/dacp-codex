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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

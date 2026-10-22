"""PiFlow Join: key-based inner/left/right/full joins with explicit suffixes."""
from dacp_piflow_support import params, columns, indexes, materialize, write_table


def process(ctx):
    p = params(ctx, ['joinMode', 'correlationColumn'])
    names = columns(p['correlationColumn'])
    modes = {'inner': 'inner', 'left': 'left outer', 'right': 'right outer', 'full_outer': 'full outer'}
    if p['joinMode'] not in modes:
        raise ValueError('joinMode must be inner, left, right or full_outer')
    left = materialize(ctx.input('left'))
    right = materialize(ctx.input('right'))
    indexes(left.schema, names)
    indexes(right.schema, names)
    output = left.join(right, keys=names, join_type=modes[p['joinMode']], right_suffix='_right')
    if len(output.column_names) != len(set(output.column_names)):
        raise ValueError('join produces ambiguous columns; rename input columns first')
    write_table(ctx, output)


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

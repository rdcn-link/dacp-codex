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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

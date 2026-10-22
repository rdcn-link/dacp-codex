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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

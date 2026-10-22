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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

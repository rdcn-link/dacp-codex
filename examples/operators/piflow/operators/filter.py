"""PiFlow Filter adapted to an explicitly documented SQLite predicate dialect."""
from dacp_piflow_support import params, predicate_indices


def process(ctx):
    p = params(ctx, ['condition'])
    source = ctx.input('source')
    out = ctx.records_output('result', source.schema)
    for batch in source.records():
        keep = predicate_indices(batch, p['condition'])
        if keep:
            out.write(batch.take(keep))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

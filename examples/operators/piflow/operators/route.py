"""PiFlow Route adapted to fixed result/matched/unmatched output ports."""
from dacp_piflow_support import params, predicate_indices


def process(ctx):
    p = params(ctx, ['condition'])
    source = ctx.input('source')
    outputs = {name: ctx.records_output(name, source.schema) for name in ('result', 'matched', 'unmatched')}
    for batch in source.records():
        keep = predicate_indices(batch, p['condition'])
        selected = set(keep)
        rest = [i for i in range(batch.num_rows) if i not in selected]
        outputs['result'].write(batch)
        if keep:
            outputs['matched'].write(batch.take(keep))
        if rest:
            outputs['unmatched'].write(batch.take(rest))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

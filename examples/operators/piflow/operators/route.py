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


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}, 'matched': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}, 'unmatched': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

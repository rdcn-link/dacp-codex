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


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

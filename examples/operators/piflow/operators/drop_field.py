"""PiFlow DropField: remove named columns from each Arrow batch."""
from dacp_piflow_support import params, columns
import pyarrow as pa


def process(ctx):
    p = params(ctx, ['columnNames'])
    names = columns(p['columnNames'])
    source = ctx.input('source')
    # Spark drop ignores absent names; retain that behavior.
    keep = [n for n in source.schema.names if n not in names]
    if not keep:
        raise ValueError('at least one output column is required')
    schema = source.schema
    out = ctx.records_output('result', pa.schema([schema.field(n) for n in keep], metadata=schema.metadata))
    for batch in source.records():
        out.write(batch.select(keep))


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    names = params.get('columnNames')
    if not isinstance(names, str) or not names or len(set(names.split(','))) != len(names.split(',')):
        raise ValueError('columnNames must contain unique names')
    names = [n.strip() for n in names.split(',')]
    if schema is not None:
        fields = [f for f in schema if f.name not in names]
        if not fields:
            raise ValueError('at least one output column is required')
        schema = pa.schema(fields, metadata=schema.metadata)
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

"""PiFlow ConvertSchema renames columns; it does not cast their types."""
import pyarrow as pa
from dacp_piflow_support import params, columns


def process(ctx):
    p = params(ctx, ['schema'])
    pairs = columns(p['schema'])
    mapping = []
    for pair in pairs:
        split = [s.strip() for s in pair.split('->')]
        if len(split) != 2 or not all(split):
            raise ValueError('schema must be old->new mappings separated by commas')
        mapping.append(split)
    source = ctx.input('source')
    schema = source.schema
    names = schema.names
    for old, new in mapping:
        names = [new if n == old else n for n in names]
    if len(set(names)) != len(names):
        raise ValueError('renaming creates duplicate columns')
    out_schema = pa.schema([field.with_name(n) for field, n in zip(schema, names)], metadata=schema.metadata)
    out = ctx.records_output('result', out_schema)
    for batch in source.records():
        out.write(pa.RecordBatch.from_arrays(batch.columns, schema=out_schema))


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

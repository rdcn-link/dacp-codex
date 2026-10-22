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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

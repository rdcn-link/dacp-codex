"""PiFlow AddUUIDStop: prepend a UUID column to each record."""
import uuid
import pyarrow as pa
from dacp_piflow_support import params


def process(ctx):
    p = params(ctx, ['column'])
    column = p['column']
    source = ctx.input('source')
    schema = source.schema
    if not isinstance(column, str) or not column or column in schema.names:
        raise ValueError('column must be a nonempty new name')
    target = pa.schema([pa.field(column, pa.string(), nullable=False), *schema], metadata=schema.metadata)
    out = ctx.records_output('result', target)
    for batch in source.records():
        ids = pa.array([uuid.uuid4().hex for _ in range(batch.num_rows)])
        out.write(pa.RecordBatch.from_arrays([ids, *batch.columns], schema=target))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

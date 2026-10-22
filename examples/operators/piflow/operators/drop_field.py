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


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

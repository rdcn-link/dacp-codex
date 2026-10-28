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


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    schema = records_schema(inputs['source']) if 'source' in inputs else None
    column = params.get('column')
    if not isinstance(column, str) or not column or (schema is not None and column in schema.names):
        raise ValueError('column must be a nonempty new name')
    if schema is not None:
        schema = pa.schema([pa.field(column, pa.string(), nullable=False), *schema], metadata=schema.metadata)
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

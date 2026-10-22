"""PiFlow SelectField: project columns in the requested order, batch by batch."""
def process(ctx):
    if set(ctx.params) != {'columnNames'} or not isinstance(ctx.params['columnNames'], str):
        raise ValueError('columnNames must be a comma-separated string')
    names = [s.strip() for s in ctx.params['columnNames'].split(',')]
    if not all(names) or len(names) != len(set(names)):
        raise ValueError('columnNames must contain unique, nonempty names')
    source = ctx.input('source')
    indexes = [source.schema.get_field_index(n) for n in names]
    if min(indexes) < 0:
        raise ValueError('unknown or ambiguous column')
    import pyarrow as pa
    schema = pa.schema([source.schema.field(i) for i in indexes], metadata=source.schema.metadata)
    out = ctx.records_output('result', schema)
    for batch in source.records():
        out.write(batch.select(indexes))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

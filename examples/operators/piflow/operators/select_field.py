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
        if any(schema.get_field_index(n) < 0 for n in names):
            raise ValueError('unknown or ambiguous column')
        schema = pa.schema([schema.field(n) for n in names], metadata=schema.metadata)
    if schema is not None:
        outputs = {p: descriptor(['records'], ['arrow'], schema, certainty='known') for p in outputs}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

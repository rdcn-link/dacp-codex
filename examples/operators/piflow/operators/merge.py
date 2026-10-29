"""Merge two record streams by field name, filling absent fields with nulls."""
from dacp_piflow_support import params


def merged_schema(left, right):
    import pyarrow as pa
    for schema in (left, right):
        if len(schema.names) != len(set(schema.names)):
            raise ValueError('merge input schema contains duplicate field names')
    fields = []
    for name in dict.fromkeys(left.names + right.names):
        a = left.field(name) if name in left.names else None
        b = right.field(name) if name in right.names else None
        if a is not None and b is not None:
            if a.type != b.type and not (pa.types.is_null(a.type) or pa.types.is_null(b.type)):
                raise ValueError('merge has incompatible types for field: ' + name)
            base = b if pa.types.is_null(a.type) else a
            fields.append(pa.field(name, base.type, nullable=a.nullable or b.nullable
                                   or pa.types.is_null(a.type) or pa.types.is_null(b.type),
                                   metadata=base.metadata))
        else:
            base = a if a is not None else b
            fields.append(pa.field(name, base.type, nullable=True, metadata=base.metadata))
    return pa.schema(fields, metadata=left.metadata)


def process(ctx):
    import pyarrow as pa
    params(ctx)
    left, right = ctx.input('left'), ctx.input('right')
    schema = merged_schema(left.schema, right.schema)
    out = ctx.records_output('result', schema)
    for stream in (left, right):
        for batch in stream.records():
            arrays = [batch.column(batch.schema.get_field_index(field.name)).cast(field.type)
                      if field.name in batch.schema.names else pa.nulls(batch.num_rows, field.type)
                      for field in schema]
            out.write(pa.RecordBatch.from_arrays(arrays, schema=schema))


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result, records_schema
    if set(inputs) != {'left', 'right'}:
        raise ValueError('unexpected input ports')
    left, right = records_schema(inputs['left']), records_schema(inputs['right'])
    schema = merged_schema(left, right) if left is not None and right is not None else None
    return result({'result': descriptor(['records'], ['arrow'], schema,
                                       'known' if schema is not None else 'unknown')})


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

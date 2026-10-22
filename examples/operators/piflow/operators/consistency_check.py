"""PiFlow ConsistencyCheck: left join reference keys and prepend a 0/1 flag."""
import pyarrow as pa
from dacp_piflow_support import params, columns, indexes, materialize, write_table


def process(ctx):
    p = params(ctx, ['flagField', 'correlationColumn'])
    names = columns(p['correlationColumn'])
    flag = p['flagField']
    left = materialize(ctx.input('left'))
    right = materialize(ctx.input('right'))
    indexes(left.schema, names)
    indexes(right.schema, names)
    if not isinstance(flag, str) or not flag or flag in left.column_names:
        raise ValueError('flagField must name a new column')
    import pickle
    counts = {}
    for row in right.select(names).to_pylist():
        if any(v is None for v in row.values()):
            continue
        key = pickle.dumps(row)
        counts[key] = counts.get(key, 0) + 1
    schema = pa.schema([pa.field(flag, pa.int32(), nullable=False), *left.schema])
    out = ctx.records_output('result', schema)
    rows = []
    for row in left.to_pylist():
        keys = {n: row[n] for n in names}
        count = 0 if any(v is None for v in keys.values()) else counts.get(pickle.dumps(keys), 0)
        # Preserve left-join multiplicity when the reference has duplicate keys.
        for _ in range(max(count, 1)):
            rows.append({flag: 0 if count else 1, **row})
            if len(rows) == 256:
                out.write(pa.RecordBatch.from_pylist(rows, schema=schema))
                rows.clear()
    if rows:
        out.write(pa.RecordBatch.from_pylist(rows, schema=schema))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

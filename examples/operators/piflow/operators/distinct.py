"""PiFlow Distinct: keep the first row per key across input batches."""
import pickle
from dacp_piflow_support import params, columns, indexes, MAX_BYTES


def process(ctx):
    p = params(ctx, optional=['columnNames'])
    source = ctx.input('source')
    names = columns(p.get('columnNames', ''), allow_empty=True) or source.schema.names
    indexes(source.schema, names)
    out = ctx.records_output('result', source.schema)
    seen, size = set(), 0
    for batch in source.records():
        keep = []
        for i, row in enumerate(batch.select(names).to_pylist()):
            key = pickle.dumps(row, protocol=4)
            if key not in seen:
                size += len(key) + 128
                if size > MAX_BYTES:
                    raise ValueError('distinct key state exceeds 128 MiB')
                seen.add(key)
                keep.append(i)
        if keep:
            out.write(batch.take(keep))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

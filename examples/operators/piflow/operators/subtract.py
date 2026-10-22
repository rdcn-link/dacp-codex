"""PiFlow Subtract: set difference over entire records (EXCEPT DISTINCT)."""
import pickle
import pyarrow as pa
from dacp_piflow_support import params, materialize, write_table


def process(ctx):
    params(ctx)
    left = materialize(ctx.input('left'))
    right = materialize(ctx.input('right'))
    if left.schema != right.schema:
        raise ValueError('subtract requires identical ordered schemas')
    excluded = {pickle.dumps(row) for row in right.to_pylist()}
    seen, keep = set(), []
    for i, row in enumerate(left.to_pylist()):
        key = pickle.dumps(row)
        if key not in excluded and key not in seen:
            seen.add(key)
            keep.append(i)
    write_table(ctx, left.take(pa.array(keep, type=pa.int64())))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

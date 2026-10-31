"""Bounded previews of ordinary Arrow batches, with no representation wire codecs."""
import json


def compact(value, limit):
    text = json.dumps(value, ensure_ascii=True, separators=(',', ':'), default=str)
    return text if len(text) <= limit else text[:limit] + ' ...'


def batch_previews(view, descriptor, batch, rows, byte_limit):
    """Yield location, text, element count and consumed payload bytes within budgets."""
    for i in range(batch.num_rows):
        if rows <= 0 or byte_limit <= 0:
            return
        if view == 'array':
            required = ('origin', 'shape', 'stride', 'values')
            if not all(name in batch.schema.names for name in required):
                raise ValueError('invalid array Arrow columns')
            scalar = batch.column('values')[i]
            if not scalar.is_valid:
                raise ValueError('null array block')
            values = scalar.values
            width = max(1, getattr(values.type, 'bit_width', 8) // 8)
            count = min(rows, byte_limit // width, len(values))
            if count == 0:
                return
            text = compact(values.slice(0, count).to_pylist(), byte_limit)
            location = {name: batch.column(name)[i].as_py() for name in required[:-1]}
            size = count * width
        elif view == 'sequence':
            symbols = batch.column('symbols')[i].as_py()
            if not isinstance(symbols, str):
                raise ValueError('invalid sequence symbols')
            payload = symbols.encode('utf-8')[:byte_limit]
            text = payload.decode('utf-8', errors='replace')
            location = {name: batch.column(name)[i].as_py() for name in ('id', 'offset', 'last')}
            count, size = 1, len(payload)
        else:
            # Preview one row, including nested values, without format-specific decoding.
            text = compact(batch.slice(i, 1).to_pylist()[0], byte_limit)
            location = {'row': i}
            count, size = 1, min(byte_limit, len(text.encode('utf-8')))
        yield location, text, count, size
        rows -= count
        byte_limit -= size

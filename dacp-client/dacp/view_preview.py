"""Bounded textual previews of the public scientific wire representations.

Unknown representation/encoding plugins retain a metadata + hexadecimal preview.
No file-format detection or format-specific dependency belongs in the shell.
"""
import json
import struct

_NUMERIC = {'int8': 'b', 'uint8': 'B', 'int16': 'h', 'uint16': 'H',
            'int32': 'i', 'uint32': 'I', 'int64': 'q', 'uint64': 'Q',
            'float32': 'f', 'float64': 'd'}


def compact(value, limit):
    text = json.dumps(value, ensure_ascii=True, separators=(',', ':'), default=str)
    return text if len(text) <= limit else text[:limit] + ' ...'


def block_preview(view, descriptor, chunk, rows, byte_limit):
    """Return preview text, displayed element count, and consumed payload bytes."""
    code = _NUMERIC.get(descriptor.get('data_type'))
    if view in ('array', 'image') and code and descriptor.get('version') == 1:
        order = descriptor.get('byte_order')
        if order not in ('little', 'big'):
            raise ValueError('invalid scientific byte order')
        width = struct.calcsize(code)
        if len(chunk.data) % width:
            raise ValueError('misaligned scientific numeric payload')
        count = min(rows, byte_limit // width, len(chunk.data) // width)
        values = [struct.unpack_from(('<' if order == 'little' else '>') + code,
                                     chunk.data, i * width)[0] for i in range(count)]
        return compact(values, byte_limit), count, count * width
    if view == 'sequence' and descriptor.get('version') == 1:
        payload = chunk.data[:byte_limit]
        text = payload.decode('utf-8', errors='replace')
        return text, 1, len(payload)
    payload = chunk.data[:byte_limit]
    return payload.hex(' '), 1, len(payload)

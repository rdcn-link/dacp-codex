"""Contract checks without Docker or wall-clock sleeps."""
import json
from unittest.mock import patch
import live_ticks


class Context:
    def __init__(self, params, stop_after=None):
        self.params = params
        self.rows = []
        self.stop_after = stop_after
    def input(self, name):
        raise AssertionError('zero-input source must not request an input')
    def bytes_output(self, name, format):
        assert (name, format) == ('result', 'ndjson')
        return self
    def write(self, value):
        if self.stop_after is not None and len(self.rows) == self.stop_after:
            raise BrokenPipeError('consumer disconnected')
        self.rows.append(json.loads(value))


with patch.object(live_ticks.time, 'sleep') as sleep:
    ctx = Context({'max_records': 3})
    live_ticks.process(ctx)
    assert [r['sequence'] for r in ctx.rows] == [1, 2, 3]
    assert sleep.call_count == 2
    assert all(r['timestamp'].endswith('+00:00') for r in ctx.rows)
with patch.object(live_ticks.time, 'sleep'):
    ctx = Context({}, stop_after=4)
    try:
        live_ticks.process(ctx)
        raise AssertionError('continuous source ignored disconnection')
    except BrokenPipeError:
        assert len(ctx.rows) == 4
for params in ({'interval_seconds':0}, {'interval_seconds':float('nan')}, {'interval_seconds':True}, {'max_records':-1}, {'max_records':True}, {'unexpected':1}):
    try:
        live_ticks.process(Context(params))
        raise AssertionError('invalid parameters accepted')
    except ValueError:
        pass
print('PASS: zero-input source, ordered output, pacing, continuous disconnection and validation')

# The decoder must emit before input EOF and preserve JSON across chunk boundaries.
import live_ticks_decode
class Decoder:
    params = {}
    def __init__(self): self.rows = []
    def input(self, name): return self
    def bytes(self):
        raw = json.dumps(dict(sequence=1, timestamp='2026-10-23T00:00:00Z', station_id='站点A', temperature_c=20.5), ensure_ascii=False).encode() + b'\n'
        cut = raw.index('站'.encode()) + 1
        yield raw[:cut]
        yield raw[cut:]
        assert len(self.rows) == 1, 'decoder waited for EOF'
        yield b'{"sequence":2,"timestamp":"now","station_id":"B","temperature_c":21}'
    def records_output(self, name, schema):
        assert name == 'result' and schema == live_ticks_decode.SCHEMA
        return self
    def write(self, batch): self.rows.extend(batch.to_pylist())
ctx = Decoder();live_ticks_decode.process(ctx)
assert [r['sequence'] for r in ctx.rows] == [1, 2] and ctx.rows[0]['station_id'] == '站点A'
print('PASS: decoder emits before EOF, handles split UTF-8 and final unterminated line')

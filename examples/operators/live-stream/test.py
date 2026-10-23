"""Contract checks without Docker or wall-clock sleeps."""
import json
from unittest.mock import patch
import live_ticks


class Context:
    def __init__(self, params, stop_after=None):
        self.params = params
        self.rows = []
        self.drained = False
        self.stop_after = stop_after
    def input(self, name):
        assert name == 'source'
        return self
    def bytes(self):
        yield b'trigger'
        self.drained = True
    def bytes_output(self, name, format):
        assert self.drained and (name, format) == ('result', 'ndjson')
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
print('PASS: bounded trigger drain, ordered output, pacing, continuous disconnection and validation')

"""A bounded-memory JSONL source paced by a monotonic clock."""
import json
import math
import time
from datetime import datetime, timezone


def process(ctx):
    unknown = set(ctx.params) - {'interval_seconds', 'max_records'}
    if unknown:
        raise ValueError('unknown parameters: ' + ', '.join(sorted(unknown)))
    interval = ctx.params.get('interval_seconds', 1.0)
    maximum = ctx.params.get('max_records', 0)
    if isinstance(interval, bool) or not isinstance(interval, (int, float)) or not math.isfinite(interval) or not 0.01 <= interval <= 60:
        raise ValueError('interval_seconds must be a number between 0.01 and 60')
    if isinstance(maximum, bool) or not isinstance(maximum, int) or maximum < 0:
        raise ValueError('max_records must be a nonnegative integer; 0 means continuous')
    output = ctx.bytes_output('result', 'ndjson')
    sequence = 0
    while maximum == 0 or sequence < maximum:
        started = time.monotonic()
        sequence += 1
        value = dict(sequence=sequence, timestamp=datetime.now(timezone.utc).isoformat(),
                     station_id='DEMO-001', temperature_c=round(20 + 5 * math.sin(sequence / 10), 2))
        output.write((json.dumps(value, separators=(',', ':'), allow_nan=False) + '\n').encode())
        if maximum == 0 or sequence < maximum:
            time.sleep(max(0.0, interval - (time.monotonic() - started)))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

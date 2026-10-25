"""Reduce hourly observation record batches into daily station statistics."""
from datetime import datetime, timezone
import json
import math
from dacp_operator import OperatorContext


def process(ctx):
    if ctx.params or ctx.input_names != ('source',):
        raise ValueError('expected source records and no parameters')
    groups = {}
    for batch in ctx.input('source').records():
        required = {'station_id', 'timestamp_utc', 'air_temperature_c', 'quality_flag'}
        if not required.issubset(batch.schema.names):
            raise ValueError('missing observation columns')
        for row in batch.to_pylist():
            if row['quality_flag'] != 0:
                continue
            station = row['station_id']
            instant = datetime.fromisoformat(row['timestamp_utc'].replace('Z', '+00:00'))
            if not station or instant.utcoffset() != timezone.utc.utcoffset(instant):
                raise ValueError('station identity and UTC timestamp required')
            value = float(row['air_temperature_c'])
            if not math.isfinite(value) or not -90 <= value <= 60:
                raise ValueError('temperature outside demo limits')
            key = station, instant.date().isoformat()
            if key not in groups:
                if len(groups) >= 100000:
                    raise ValueError('too many station-day groups for this example')
                groups[key] = [0, 0.0, value, value]
            stats = groups[key]
            stats[0] += 1
            stats[1] += value
            stats[2] = min(stats[2], value)
            stats[3] = max(stats[3], value)
    output = ctx.bytes_output('result', 'ndjson')
    for (station, day), (count, total, low, high) in sorted(groups.items()):
        output.write((json.dumps(dict(station_id=station, date=day, sample_count=count, temperature_sum_c=total, min_temperature_c=low, max_temperature_c=high), allow_nan=False) + '\n').encode())


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    return result({'result': descriptor(['bytes'], ['ndjson'], None, certainty='known')})


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

"""Join remote daily statistics with local station/month climate references."""
from datetime import date
import codecs
import json
import math
from xstream_operator import OperatorContext


def json_lines(chunks):
    decoder = codecs.getincrementaldecoder('utf-8')()
    pending = ''
    for chunk in chunks:
        pending += decoder.decode(chunk)
        while '\n' in pending:
            line, pending = pending.split('\n', 1)
            if len(line) > 65536:
                raise ValueError('statistics line too long')
            if line.strip():
                yield json.loads(line)
        if len(pending) > 65536:
            raise ValueError('statistics line too long')
    pending += decoder.decode(b'', final=True)
    if pending.strip():
        yield json.loads(pending)


def process(ctx):
    if ctx.params or ctx.input_names != ('baselines', 'statistics'):
        raise ValueError('expected baselines and statistics, no parameters')
    references = {}
    for batch in ctx.input('baselines').records():
        for row in batch.to_pylist():
            key = row['station_id'], int(row['reference_month'])
            if key in references or len(references) >= 10000:
                raise ValueError('duplicate or excessive station references')
            reference = float(row['reference_temperature_c'])
            if not math.isfinite(reference):
                raise ValueError('invalid reference temperature')
            references[key] = row
    output = ctx.bytes_output('result', 'ndjson')
    for row in json_lines(ctx.input('statistics').bytes()):
        key = row['station_id'], date.fromisoformat(row['date']).month
        if key not in references:
            raise ValueError('station-month reference not found')
        station = references[key]
        count = int(row['sample_count'])
        if count <= 0:
            raise ValueError('positive sample count required')
        mean = float(row['temperature_sum_c']) / count
        reference = float(station['reference_temperature_c'])
        result = dict(station_id=row['station_id'], station_name=station['station_name'], date=row['date'],
                      latitude=station['latitude'], longitude=station['longitude'], sample_count=count,
                      mean_temperature_c=round(mean, 3), reference_temperature_c=reference,
                      anomaly_c=round(mean - reference, 3), reference_period=station['reference_period'])
        output.write((json.dumps(result, allow_nan=False) + '\n').encode())


def infer_outputs(inputs, params):
    from xstream_operator import descriptor, result
    return result({'result': descriptor(['bytes'], ['ndjson'], None, certainty='known')})


def preview_outputs(inputs, params, limits):
    from xstream_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from xstream_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

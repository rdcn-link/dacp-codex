#!/usr/bin/env python3
"""Check climate operators against source data, including chunked UTF-8/NDJSON input.

Requires pyarrow and the local operator SDK. --netcdf also checks dimension/CSV consistency.
"""
import argparse
import csv
from collections import defaultdict
from datetime import datetime
import importlib.util
import io
import json
from pathlib import Path
import sys
import tempfile
import pyarrow as pa

ROOT = Path(__file__).resolve().parents[1]
from xstream_operator import OperatorContext


def module(name):
    path = ROOT / f'examples/union/operators/{name}/1.0/{name}.py'
    spec = importlib.util.spec_from_file_location(name, path)
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def run(operator, ports):
    with tempfile.TemporaryDirectory() as temp:
        directory = Path(temp)
        manifest, streams, events = [], {}, []
        for port, (representation, batches) in ports.items():
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, batches[0].schema) as writer:
                for batch in batches:
                    writer.write_batch(batch)
            streams[port] = io.BytesIO(sink.getvalue().to_pybytes())
            manifest.append(dict(port=port, representation=representation, format='csv' if representation == 'records' else 'ndjson', file=port+'.arrow'))
        (directory / 'manifest.json').write_text(json.dumps(manifest))
        (directory / 'params.json').write_text('{}')
        with OperatorContext(['operator', str(directory), str(directory / 'params.json')], input_streams=streams, emit=lambda h,p: events.append((h,p))) as ctx:
            module(operator).process(ctx)
        assert events[0][0]['type'] == 'start' and events[-1][0]['type'] == 'end'
        return b''.join(p for h,p in events if h['type']=='data')


def batches(rows, size=127):
    return [pa.RecordBatch.from_pylist(rows[i:i+size]) for i in range(0,len(rows),size)]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--netcdf', action='store_true')
    args = parser.parse_args()
    data = ROOT / 'examples/union/data'
    references = list(csv.DictReader((data / 'station_baselines.csv').open()))
    for r in references:
        for k in ['latitude','longitude','elevation_m','reference_temperature_c']:
            r[k] = float(r[k])
        r['reference_month'] = int(r['reference_month'])
    for region in ['east','west']:
        rows = list(csv.DictReader((data / f'{region}_observations.csv').open()))
        expected = defaultdict(list)
        for r in rows:
            r['air_temperature_c'] = float(r['air_temperature_c'])
            r['quality_flag'] = int(r['quality_flag'])
            if r['quality_flag'] == 0:
                expected[r['station_id'], r['timestamp_utc'][:10]].append(r['air_temperature_c'])
        statistics = run('daily_temperature_stats', {'source': ('records', batches(rows))})
        # Force transport fragments through the middle of NDJSON lines.
        chunks = [statistics[i:i+19] for i in range(0,len(statistics),19)]
        wire = pa.RecordBatch.from_arrays([pa.array(chunks, type=pa.binary())], names=['data'])
        result = run('station_temperature_anomaly', {'baselines': ('records', batches(references)), 'statistics': ('bytes',[wire])})
        output = [json.loads(line) for line in result.splitlines()]
        assert len(output) == len(expected) == 168
        by_station = {r['station_id']:r for r in references}
        for row in output:
            values = expected[row['station_id'],row['date']]
            mean = sum(values)/len(values)
            reference = by_station[row['station_id']]['reference_temperature_c']
            assert row['sample_count'] == len(values)
            assert row['mean_temperature_c'] == round(mean,3)
            assert row['anomaly_c'] == round(mean-reference,3)
        assert {r['sample_count'] for r in output} == {23,24}
        assert all(r['anomaly_c']>0 for r in output) if region=='east' else any(r['anomaly_c']<0 for r in output)
        print(region, ': 4032 source records -> 168 verified station-day anomalies')
        if args.netcdf:
            from netCDF4 import Dataset
            with Dataset(data/f'{region}_air_temperature.nc') as nc:
                assert nc.variables['air_temperature'].dimensions == ('time','latitude','longitude')
                assert nc.variables['air_temperature'].shape == (336,3,4)
                assert nc.variables['air_temperature'].units == 'degree_Celsius'
                for index,row in enumerate(rows):
                    hour,cell = divmod(index,12)
                    y,x = divmod(cell,4)
                    assert abs(float(nc.variables['air_temperature'][hour,y,x])-row['air_temperature_c'])<0.00001
                    assert int(nc.variables['quality_flag'][hour,y,x])==row['quality_flag']
            print(region, ': NetCDF dimensions, units, values and quality flags match source CSV')
    helper=module('station_temperature_anomaly')
    encoded='{"station":"站点甲"}\n{"station":"乙"}'.encode()
    assert list(helper.json_lines([bytes([v]) for v in encoded]))==[{'station':'站点甲'},{'station':'乙'}]
    try:
        list(helper.json_lines([b'x'*65537]))
        raise AssertionError('oversized NDJSON line accepted')
    except ValueError:
        pass
    print('Climate example contracts passed')


if __name__=='__main__':
    main()

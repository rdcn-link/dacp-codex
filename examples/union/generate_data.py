"""Generate deterministic SYNTHETIC climate teaching data, never real observations.

Only regeneration requires netCDF4; DACP nodes and the example operators do not.
"""
import argparse
import csv
from datetime import datetime, timedelta, timezone
import math
from pathlib import Path

START = datetime(2026, 10, 1, tzinfo=timezone.utc)


def generate(directory, days=14):
    from netCDF4 import Dataset
    directory.mkdir(parents=True, exist_ok=True)
    stations = []
    for region, longitude_base, offset in [('east', 110.0, 1.5), ('west', 100.0, -0.75)]:
        rows = []
        with Dataset(directory / f'{region}_air_temperature.nc', 'w', format='NETCDF3_CLASSIC') as nc:
            nc.title = f'Synthetic {region} hourly air temperature for DACP demonstration'
            nc.source = 'Deterministic mathematical simulation; not measured data'
            nc.createDimension('time', days * 24)
            nc.createDimension('latitude', 3)
            nc.createDimension('longitude', 4)
            time = nc.createVariable('time', 'f8', ('time',))
            time.units = 'hours since 2026-10-01 00:00:00 UTC'
            time.calendar = 'standard'
            time.standard_name = 'time'
            time[:] = list(range(days * 24))
            lat = nc.createVariable('latitude', 'f4', ('latitude',))
            lat.units = 'degrees_north'
            lat.standard_name = 'latitude'
            lat[:] = [33.0, 34.0, 35.0]
            lon = nc.createVariable('longitude', 'f4', ('longitude',))
            lon.units = 'degrees_east'
            lon.standard_name = 'longitude'
            lon[:] = [longitude_base + x for x in range(4)]
            temperature = nc.createVariable('air_temperature', 'f4', ('time', 'latitude', 'longitude'))
            temperature.units = 'degree_Celsius'
            temperature.standard_name = 'air_temperature'
            temperature.long_name = 'Synthetic near-surface air temperature'
            quality = nc.createVariable('quality_flag', 'i1', ('time', 'latitude', 'longitude'))
            quality.long_name = 'Synthetic quality control flag: 0 accepted, 1 rejected'
            for hour in range(days * 24):
                stamp = START + timedelta(hours=hour)
                grid, flags = [], []
                for y in range(3):
                    values, mask = [], []
                    for x in range(4):
                        index = y * 4 + x
                        station = f'{region.upper()}{index + 1:03}'
                        baseline = 16.0 - y * 0.8 + x * 0.2
                        value = round(baseline + offset + 4 * math.sin(2 * math.pi * (hour % 24) / 24) + (hour // 24) * 0.05, 3)
                        flag = int(hour % 24 == 3 and index % 3 == 0)
                        rows.append([station, stamp.isoformat().replace('+00:00', 'Z'), value, flag])
                        values.append(value)
                        mask.append(flag)
                        if hour == 0:
                            stations.append([station, f'{region.title()} research station {index + 1}', 33.0 + y, longitude_base + x, 100 + 20 * index, 10, round(baseline, 2), 'synthetic_1991_2020_reference'])
                    grid.append(values)
                    flags.append(mask)
                temperature[hour, :, :] = grid
                quality[hour, :, :] = flags
        with (directory / f'{region}_observations.csv').open('w', newline='') as stream:
            writer = csv.writer(stream, lineterminator='\n')
            writer.writerow(['station_id', 'timestamp_utc', 'air_temperature_c', 'quality_flag'])
            writer.writerows(rows)
    with (directory / 'station_baselines.csv').open('w', newline='') as stream:
        writer = csv.writer(stream, lineterminator='\n')
        writer.writerow(['station_id', 'station_name', 'latitude', 'longitude', 'elevation_m', 'reference_month', 'reference_temperature_c', 'reference_period'])
        writer.writerows(stations)
    print(f'Generated {days * 24 * 12} observations per region, 24 station references, and two structured NetCDF files in {directory}')


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, default=Path(__file__).parent / 'data')
    parser.add_argument('--days', type=int, default=14)
    args = parser.parse_args()
    if not 1 <= args.days <= 31:
        parser.error('--days must be between 1 and 31 (the reference month is October)')
    generate(args.output, args.days)

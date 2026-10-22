#!/usr/bin/env python3
"""Run one Scala scientific operator through the actual DACP Arrow IPC contract."""
import argparse
import io
import json
from pathlib import Path
import struct
import subprocess
import tempfile

import pyarrow as pa

HERE = Path(__file__).resolve().parent
HOME = HERE.parent
ROOT = HOME.parents[2]


def execute(name, params, inputs, spark_home, docker=False):
    jar = HOME / 'target/spark' / name / ('dacp-ecoflow-' + name + '-1.0.jar')
    with tempfile.TemporaryDirectory(prefix='dacp-ecoflow-') as tmp:
        directory = Path(tmp)
        directory.chmod(0o755)
        entries = []
        for port, table in inputs.items():
            with (directory / (port + '.arrow')).open('wb') as f:
                with pa.ipc.new_stream(f, table.schema) as writer:
                    writer.write_table(table, max_chunksize=2)
            entries.append(dict(port=port, file=port + '.arrow', representation='records', format='arrow'))
        (directory / 'manifest.json').write_text(json.dumps(entries))
        (directory / 'params.json').write_text(json.dumps(params))
        if docker:
            command = ['sudo', '-n', '-u', 'cnic', '-g', 'docker', 'docker', 'run', '--rm', '--network', 'none',
                '--read-only', '--memory', '1g', '--pids-limit', '128', '--tmpfs', '/tmp:rw,noexec,nosuid,nodev,size=512m',
                '-v', str(directory) + ':/input:ro', '-v', str(jar) + ':/operator/op.jar:ro',
                '--entrypoint', '/opt/java/bin/java', 'dacp/operator-ecoflow-spark:1.0',
                '--add-opens=java.base/java.nio=ALL-UNNAMED', '-Dio.netty.tryReflectionSetAccessible=true',
                '-cp', '/operator/op.jar:/opt/lib/*', 'dacp.ecoflow.Op_' + name, 'operator', '/input', '/input/params.json']
        else:
            api = ROOT / 'dacp-plugin-api/target/scala-2.13/dacp-plugin-api_2.13-0.1.0-SNAPSHOT.jar'
            command = ['java', '-Xmx640m', '--add-opens=java.base/java.nio=ALL-UNNAMED',
                '--add-opens=java.base/sun.nio.ch=ALL-UNNAMED', '-Dio.netty.tryReflectionSetAccessible=true',
                '-cp', str(jar) + ':' + str(spark_home / 'deps/jars/*') + ':' + str(api),
                'dacp.ecoflow.Op_' + name, 'operator', tmp, str(directory / 'params.json')]
        result = subprocess.run(command, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr.decode(errors='replace')[-12000:])
        data = result.stdout
        frames = []
        while data:
            h, n = struct.unpack('>II', data[:8])
            assert h <= 65536 and n <= 8 * 1024 * 1024, repr(data[:300])
            frames.append((json.loads(data[8:8+h]), data[8+h:8+h+n]))
            data = data[8+h+n:]
        assert frames[0][0]['type'] == 'start' and frames[-1][0]['type'] == 'end'
        tables = [pa.ipc.open_stream(payload).read_all() for header, payload in frames if header['type'] == 'data']
        if tables:
            return pa.concat_tables(tables).to_pylist()
        return []


def verify(name, spark_home, docker=False):
    if name == 'add_day_id':
        table = pa.table(dict(year=[2026, 2026, 2026], month=[10, 10, 10], day=[2, 1, 1], hour=[0., 1., 0.], value=[3., 2., 1.]))
        actual = execute(name, dict(partitionNum='1'), dict(source=table), spark_home, docker)
        actual.sort(key=lambda r: r['id'])
        assert [(r['id'], r['day_id'], r['value']) for r in actual] == [(1, 1, 1.), (2, 1, 2.), (3, 2, 3.)]
    elif name == 'add_day_state':
        table = pa.table(dict(year=[2026]*5, month=[1]*5, day=[1]*5, day_id=[1]*5, hour=[5., 6., 12., 18., 19.]))
        rows = execute(name, dict(longitude='120', latitude='0'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['hour'])
        assert [r['day_status'] for r in rows] == ['night', 'night', 'day', 'night', 'night']
    elif name == 'single_field_threshold_clean':
        table = pa.table(dict(temperature=[-91., -90., 60., 61., 20.], quality=[0, 0, 0, 0, 1]))
        p = dict(fields='temperature', maxValue='60', minValue='-90', flagValue='-9999', additionalCondition='quality = 0')
        rows = execute(name, p, dict(source=table), spark_home, docker)
        assert [float(r['temperature']) for r in rows] == [-9999., -90., 60., -9999., -9999.]
        assert [r['quality'] for r in rows] == [0, 0, 0, 0, 1]
    elif name == 'batch_field_threshold_clean':
        table = pa.table(dict(temperature=[-91., -90., 60., 61.], quality=[0, 1, 0, 1]))
        thresholds = pa.table(dict(field=['temperature'], min_value=[-90.], max_value=[60.], flag_value=[-9999.]))
        rows = execute(name, {}, dict(left=table, right=thresholds), spark_home, docker)
        assert [float(r['temperature']) for r in rows] == [-9999., -90., 60., -9999.]
        assert [r['quality'] for r in rows] == [0, 1, 0, 1]
    elif name == 'mdv_g':
        table = pa.table(dict(day_id=[1, 2, 3], hour=[12., 12., 12.], value=[2., -9999., 4.]))
        rows = execute(name, dict(fields='value', mdv_num='3', qualityControlMarkerValue='-9999'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['day_id'])
        assert [r['value'] for r in rows] == [2., 3., 4.]
    elif name == 'mdv_i':
        table = pa.table(dict(day_id=[1, 2, 3], hour=[12., 12., 12.], value=[2., -9999., 4.], other=[8., -9999., 10.]))
        rows = execute(name, dict(fields='value,other', mdv_num='3', qualityControlMarkerValue='-9999'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['day_id'])
        assert [r['value'] for r in rows] == [2., 2., 4.]
        assert [r['other'] for r in rows] == [8., 6., 10.]
        assert [r['zone'] for r in rows] == [1, 1, 1]
    elif name == 'front_and_back_values_are_valid':
        table = pa.table(dict(day_id=[1, 2, 3, 4], hour=[12.]*4, value=[2., -1., 4., -1.]))
        rows = execute(name, dict(fields='value', qualityControlMarkerValue='-1'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['day_id'])
        assert [r['value'] for r in rows] == [2., 3., 4., -1.]
    elif name == 'current_and_post_value_interpolation':
        table = pa.table(dict(year=[2026]*6, month=[1]*6, day=list(range(1,7)), hour=[12.]*6, value=[8., -1., -1., 2., -1., 0.]))
        rows = execute(name, dict(fields='value', qualityControlMarkerValue='-1'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['day'])
        assert [r['value'] for r in rows] == [8., 6., 4., 2., -1., 0.]
    elif name == 'wpl_correction':
        import math
        values = dict(Fc=1., Le=.02, Hs=.5, co2_Avg=700., h2o_Avg=10., Ts_Avg=20., rho_a_Avg=1.2)
        table = pa.table({k:[v] for k,v in values.items()})
        rows = execute(name, {k:k for k in values}, dict(source=table), spark_home, docker)
        pv, pa_air, pc = 10000., 1200000., 700.
        pd = pa_air-pv
        temperature = 293.15/(1+.51*pv/pa_air)
        expected = [1.+pc*28.966/(pd*18)*.02+(1+pv*28.966/(pd*18))*pc/temperature*.5,
                    2440*(1+pv*28.966/(pd*18))*(.02+pv/temperature/1000*.5),
                    1.00467*pa_air*.5/1000]
        assert all(math.isclose(float(rows[0][k]), v, abs_tol=.000006) for k,v in zip(['Fc','Le','Hs'], expected))
    elif name in ('dr_coordinate_rotation', 'pf_coordinate_rotation'):
        p = json.loads((HOME / 'specs.json').read_text())[name]['params']
        values = {k:1. for k in p if k not in ['Fc','Le','Hs','u_star']}
        values.update(Ux_Avg=3., Uy_Avg=0., Uz_Avg=0., cov_Uz_co2=2., cov_Uz_h2o=3., cov_Uz_Ts=4., cov_Uz_Ux=3., cov_Uz_Uy=4.)
        if name == 'pf_coordinate_rotation':
            data = {k:[v]*6 for k,v in values.items()}
            data.update(Ux_Avg=[1.,2.,1.,3.,2.,3.], Uy_Avg=[0.,0.,1.,2.,3.,1.], Uz_Avg=[1.]*6)
            table = pa.table(data)
        else:
            table = pa.table({k:[v] for k,v in values.items()})
        rows = execute(name, p, dict(source=table), spark_home, docker)
        expected = [2., 3., 4., 5**.5]
        assert all(abs(float(row[k])-v)<.000006 for row in rows for k,v in zip(['Fc','Le','Hs','u_star'],expected))
    elif name == 'wild_spot_culling_mdv_i':
        table = pa.table(dict(id=[1,2,3,4,5], value=[1.,1.,1.,1.,100.]))
        rows = execute(name, dict(fields='value', mdv_num='5', multiple='1', statisticalType='stddev', flagValue='-9999'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['id'])
        assert [r['value'] for r in rows] == [1.,1.,1.,1.,-9999.]
    elif name == 'wild_spot_culling_mdv_g':
        table = pa.table(dict(id=[1,2,3,4,5], value=[1.,1.,100.,1.,1.]))
        rows = execute(name, dict(fields='value', mdv_num='5', multiple='2', statisticalType='stddev', flagValue='-9999'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['id'])
        assert [r['value'] for r in rows] == [1.,1.,-9999.,1.,1.]
    elif name == 'storage_item_ec':
        table = pa.table(dict(day_id=[1,1,1], hour=[0.,.5,1.], Fc=[1.,1.,1.], co2_Avg=[700.,710.,730.]))
        rows = execute(name, dict(Fc='Fc', co2_Avg='co2_Avg', para_H='18', flagValue='-9999'), dict(source=table), spark_home, docker)
        rows.sort(key=lambda r:r['hour'])
        assert [float(r['Fc']) for r in rows] == [1., 1.1, 1.2]
    elif name == 'rainfall_data_culling':
        dates = dict(year=[2026]*4,month=[1]*4,day=[1]*4,hour=[0.,.5,1.,1.5])
        flux = pa.table(dict(**dates,Fc=[1.,2.,3.,4.]))
        weather = pa.table(dict(**dates,rain=[0.,1.,0.,0.]))
        rows = execute(name, dict(fields='Fc', rainFallField='rain', flagValue='-9999'), dict(left=flux,right=weather), spark_home, docker)
        rows.sort(key=lambda r:r['hour'])
        assert [float(r['Fc']) for r in rows] == [1.,-9999.,-9999.,4.]
    elif name == 'storage_item_pm_hs':
        dates = dict(year=[2026]*2,month=[1]*2,day=[1]*2,hour=[0.,.5])
        flux = pa.table(dict(**dates,Hs=[10.,10.],density=[1.2,1.2]))
        weather = pa.table(dict(**dates,**{'t'+str(i):[20.,21.] for i in range(1,8)}))
        p = json.loads((HOME / 'specs.json').read_text())[name]['params']
        rows = execute(name, p, dict(left=flux,right=weather), spark_home, docker)
        rows.sort(key=lambda r:r['hour'])
        assert abs(float(rows[0]['Hs'])-10.)<.000006
        assert abs(float(rows[1]['Hs'])-(10+7/1800*1004.67*1.2))<.000006
    elif name == 'storage_item_pm_le':
        dates = dict(year=[2026]*2,month=[1]*2,day=[1]*2,hour=[0.,.5])
        flux = pa.table(dict(**dates,LE=[10.,10.]))
        weather = pa.table(dict(**dates,**{'t'+str(i):[20.,20.] for i in range(1,8)}, **{'v'+str(i):[1.,2.] for i in range(1,8)}))
        p = json.loads((HOME / 'specs.json').read_text())[name]['params']
        rows = execute(name, p, dict(left=flux,right=weather), spark_home, docker)
        rows.sort(key=lambda r:r['hour'])
        assert abs(float(rows[0]['LE'])-10.)<.000006
        assert abs(float(rows[1]['LE'])-(10+7*216.5/293.15/1800*2440))<.000006
    else:
        raise ValueError('no scientific fixture: ' + name)
    print('PASS Scala Arrow contract: ' + name, flush=True)


if __name__ == '__main__':
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('name')
    p.add_argument('--spark-home', type=Path, required=True)
    p.add_argument('--docker', action='store_true')
    args = p.parse_args()
    verify(args.name, args.spark_home, args.docker)

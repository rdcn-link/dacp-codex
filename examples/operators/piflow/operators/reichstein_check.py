"""Bounded Reichstein screening adapted from EcoFlow's six-group/twenty-bin algorithm."""
import math
import statistics
import pyarrow as pa
from dacp_piflow_support import params, materialize, write_table, indexes


def process(ctx):
    p=params(ctx,['soilTemperatureField','frictionWindSpeedField','FC','flagValue','percentFlux'])
    soil,wind,flux=p['soilTemperatureField'],p['frictionWindSpeedField'],p['FC']
    flag,percent=p['flagValue'],p['percentFlux']
    if any(not isinstance(n,str) or not n for n in (soil,wind,flux)) or len({soil,wind,flux})!=3:
        raise ValueError('three distinct column names required')
    if type(flag) is not int or not -2**31<=flag<2**31 or type(percent) not in (int,float) or not 0<percent<=1:
        raise ValueError('flagValue must be int32; percentFlux must be numeric in (0,1]')
    table=materialize(ctx.input('source'))
    indexes(table.schema,[soil,wind,flux,'day_status'])
    for name in (soil,wind,flux):
        typ=table.schema.field(name).type
        if not (pa.types.is_integer(typ) or pa.types.is_floating(typ)):
            raise ValueError('temperature, wind and flux columns must be numeric')
    rows=table.to_pylist()
    valid=[r for r in rows if all(r[n] is not None and math.isfinite(r[n]) and r[n]!=flag for n in (soil,wind,flux))]
    if len(valid)<120:
        raise ValueError('at least120 valid observations required for six temperature groups')
    valid.sort(key=lambda r:r[soil])
    size=len(valid)//6
    thresholds=[]
    for i in range(6):
        group=valid[i*size:(i+1)*size if i<5 else len(valid)]
        xs=[float(r[soil]) for r in group];ys=[float(r[wind]) for r in group]
        mx,my=statistics.mean(xs),statistics.mean(ys)
        xx=sum((x-mx)**2 for x in xs);yy=sum((y-my)**2 for y in ys)
        if xx==0 or yy==0:
            continue
        correlation=sum((x-mx)*(y-my) for x,y in zip(xs,ys))/math.sqrt(xx*yy)
        if correlation>=.4:
            continue
        low,high=min(ys),max(ys)
        width=(high-low)/20
        ordered=sorted(group,key=lambda r:r[wind])
        for b in range(1,21):
            remaining=[r[flux] for r in group if r[wind]>=low+b*width]
            if not remaining:
                continue
            average=statistics.mean(remaining)
            candidates=[r for r in ordered if low+(b-1)*width<=r[wind]<(low+b*width) and r[flux]>average*percent]
            if candidates:
                thresholds.append(float(candidates[0][wind]))
                break
    if not thresholds:
        raise ValueError('no valid u-star critical threshold found')
    threshold=statistics.median(thresholds)
    values=[flag if r['day_status']=='night' and r[wind] is not None and r[wind]<threshold else r[flux] for r in rows]
    index=table.schema.get_field_index(flux)
    write_table(ctx,table.set_column(index,table.schema.field(flux),pa.array(values,type=table.schema.field(flux).type)))


def infer_outputs(inputs, params):
    import pyarrow as pa
    from dacp_operator import descriptor, result, records_schema
    expected = {'source'}
    if set(inputs) != expected:
        raise ValueError('unexpected input ports')
    outputs = {'result': {'representations': ['records'], 'formats': ['arrow'], 'certainty': 'unknown'}}
    return result(outputs)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs)

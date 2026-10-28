#!/usr/bin/env python3
"""Verify real output previews of every currently published operator without Docker or files."""
import base64, importlib.util, json, pathlib, sys
import pyarrow as pa
ROOT=pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'dacp-operator-sdk/python'))
from dacp_operator import descriptor, sample_bytes, sample_records
from dacp_operator.inference import evaluate_inference, preview_process
HERE=ROOT/'examples/operators/piflow/operators'
def load(name,alias=None):
 spec=importlib.util.spec_from_file_location(alias or name,HERE/(name+'.py'));m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;spec.loader.exec_module(m);return m
load('support','dacp_piflow_support');load('execute_sql_stop','dacp_piflow_sql')
limits={'max_rows':2,'max_bytes':1024}
schema=pa.schema([('station',pa.string()),('amount',pa.int64())])
rows=[{'station':'A','amount':1},{'station':'B','amount':3}]
source=descriptor(['records'],['arrow'],schema,'known',sample=sample_records(rows))
right=descriptor(['records'],['arrow'],pa.schema([('station',pa.string()),('quality',pa.int64())]),'known',sample=sample_records([{'station':'A','quality':5},{'station':'B','quality':8}]))
def run(name,inputs,params):
 m=load(name)
 assert callable(m.preview_outputs)
 raw=evaluate_inference(m.infer_outputs,json.dumps({'inputs':inputs,'params':params,'preview':limits}).encode(),m.preview_outputs)
 value=json.loads(raw);assert not value['diagnostics'],value
 sample=value['outputs']['result']['sample'];assert len(json.dumps(sample).encode())<=1024
 assert len(sample.get('rows',[]))<=2
 return sample
csv=run('csv_parser',{'source':descriptor(['bytes'],['csv'],certainty='known',sample=sample_bytes(b'station,amount\nA,1\nB,3\n',format='csv'))},{})
assert csv['rows']==[{'station':'A','amount':'1'},{'station':'B','amount':'3'}]
assert run('filter',{'source':source},{'condition':'amount > 1'})['rows']==[rows[1]]
assert run('merge',{'left':source,'right':source},{})['truncated']
joined=run('join',{'left':source,'right':right},{'joinMode':'inner','correlationColumn':'station'})
assert {r['station'] for r in joined['rows']}=={'A','B'}
assert run('execute_sql_stop',{'source':source},{'sql':'SELECT station, amount*2 AS doubled FROM source','viewName':'source'})['rows'][1]['doubled']==6
saved=run('csv_save',{'source':source},{})
assert base64.b64decode(saved['data_base64'])==b'station,amount\nA,1\nB,3\n'
# Unknown inferred schemas can still propagate actual samples, without fabricating schemas.
unknown=descriptor(['records'],['arrow'],sample=csv)
assert run('filter',{'source':unknown},{'condition':"station = 'B'"})['rows']==[{'station':'B','amount':'3'}]
try:evaluate_inference(lambda *_: {},b'{"inputs":{},"params":{}}')
except TypeError:pass
else:raise AssertionError('missing mandatory preview accepted')
# A source with no natural end is stopped when the bounded output fills.
def forever(ctx):
 out=ctx.records_output('result',schema)
 while True:out.write(pa.RecordBatch.from_pylist(rows,schema=schema))
assert preview_process(forever,{}, {},limits)['result']['truncated']
print('All six repository operator previews passed; row/byte limits, unknown-schema propagation and mandatory registration verified')

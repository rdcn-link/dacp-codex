#!/usr/bin/env python3
"""Exercise shared-interpreter execution with real Arrow streams and bounded links."""
import base64
import json
from pathlib import Path
import shutil
import struct
import subprocess
import sys
import tempfile
import unittest
import pyarrow as pa
ROOT=Path(__file__).resolve().parents[1]
SOURCE='''from dacp_operator import OperatorContext
def process(ctx):
    source=ctx.input('source')
    if source.representation=='records':
        out=ctx.records_output('result',source.schema)
        for batch in source.records():out.write(batch)
    else:
        out=ctx.bytes_output('result','text')
        for chunk in source.bytes():out.write(chunk.upper())
'''
def decode(data):
    frames=[]
    while data:
        h,n=struct.unpack('>II',data[:8]);frames.append((json.loads(data[8:8+h]),data[8+h:8+h+n]));data=data[8+h+n:]
    return frames
class PythonGroup(unittest.TestCase):
    def run_group(self,records=False,mismatch=False):
        with tempfile.TemporaryDirectory() as tmp:
            d=Path(tmp);(d/'ops.py').write_text(SOURCE)
            shutil.copyfile(ROOT/'dacp-operator-sdk/python/dacp_operator/__init__.py',d/'group-sdk.py')
            table=pa.table({'value':[1,None,3,4]}) if records else pa.table({'data':pa.array([b'abc'*30000]*12,type=pa.binary())})
            with (d/'0.arrow').open('wb') as raw:
                with pa.ipc.new_stream(raw,table.schema) as w:w.write_table(table,max_chunksize=2)
            repr='records' if records else 'bytes';fmt='arrow' if records else 'text'
            contract=dict(representations=[repr],formats=['wrong'] if mismatch else [fmt])
            if records:contract['schema']=base64.b64encode(table.schema.serialize().to_pybytes()).decode()
            meta=dict(port='i0',file='0.arrow',representation=repr,format=fmt)
            if records:meta['schema']=base64.b64encode(table.schema.serialize().to_pybytes()).decode()
            (d/'manifest.json').write_text(json.dumps([meta]))
            steps=[]
            for id,binding in [('a',dict(port='i0')),('b',dict(task='a',port='result'))]:
                steps.append(dict(id=id,artifact=str(d/'ops.py'),entrypoint='ops',params={},bindings={'source':binding},inputs={'source':contract},outputs={'result':contract}))
            (d/'group.json').write_text(json.dumps(dict(steps=steps,outputs={'o0':dict(task='b',port='result')})))
            p=subprocess.run([sys.executable,str(ROOT/'plugins/dacp-plugin-operator-executors/src/main/resources/python-group.py'),str(d)],capture_output=True,timeout=20)
            if mismatch:
                self.assertNotEqual(p.returncode,0);return
            self.assertEqual(p.returncode,0,p.stderr.decode())
            frames=decode(p.stdout);states=[h for h,_ in frames if h['type']=='task_state'];self.assertEqual(len(states),4);self.assertEqual(len({h['runtime_pid'] for h in states}),1)
            outputs=[(h,b) for h,b in frames if not h['type'].startswith('task_')]
            self.assertEqual([h['type'] for h,_ in outputs][::len(outputs)-1],['start','end'])
            chunks=[b for h,b in outputs if h['type']=='data']
            if records:
                batches=[b for chunk in chunks for b in pa.ipc.open_stream(chunk)]
                self.assertEqual(pa.Table.from_batches(batches).to_pydict(),table.to_pydict())
                self.assertTrue(any(h.get('records',0)>0 for h,_ in frames if h['type']=='task_metric'))
            else:self.assertEqual(b''.join(chunks),b'ABC'*30000*12)
    def test_large_byte_stream_same_pid(self):self.run_group()
    def test_records_preserve_schema_nulls_and_batches(self):self.run_group(records=True)
    def test_contract_failure_terminates_without_deadlock(self):self.run_group(mismatch=True)
if __name__=='__main__':unittest.main()

#!/usr/bin/env python3
"""Golden execution contracts for every common operator, including protocol framing."""
import os
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest
ROOT=Path(__file__).resolve().parents[1]
if os.environ.get('DACP_COMMON_TEST_PACKAGE'):
    from dacp_common.catalog import CATALOG,execute
else:
    sys.path.insert(0,str(ROOT/'examples/operators/common'))
    sys.path.insert(0,str(ROOT/'dacp-operator-sdk/python'))
    from catalog import CATALOG,execute
from dacp_operator import OperatorContext
import pyarrow as pa

def run(name,sample,params):
    spec=CATALOG[name]
    with tempfile.TemporaryDirectory() as tmp:
        p=Path(tmp);records=spec['representation']=='records'
        table=pa.table(sample) if records else pa.table({'data':pa.array([sample[i:i+2] for i in range(0,len(sample),2)],type=pa.binary())})
        with (p/'input.arrow').open('wb') as f:
            with pa.ipc.new_stream(f,table.schema) as w:
                w.write_table(table,max_chunksize=2)
        (p/'manifest.json').write_text(json.dumps([dict(port='source',file='input.arrow',representation=spec['representation'],format='arrow' if records else 'binary')]))
        (p/'params.json').write_text(json.dumps(params));out=io.BytesIO()
        with OperatorContext(['operator',str(p),str(p/'params.json')],out) as ctx:execute(name,ctx)
        frames=[];b=out.getvalue()
        while b:
            h,n=struct.unpack('>II',b[:8]);frames.append((json.loads(b[8:8+h]),b[8+h:8+h+n]));b=b[8+h+n:]
        assert frames[0][0]['type']=='start' and frames[-1][0]['type']=='end'
        assert all(h['port']=='result' for h,_ in frames)
        chunks=[v for h,v in frames if h['type']=='data']
        if records:
            schema=pa.ipc.read_schema(pa.BufferReader(__import__('base64').b64decode(frames[0][0]['schema'])))
            batches=[b for chunk in chunks for b in pa.ipc.open_stream(chunk)]
            return pa.Table.from_batches(batches,schema=schema).to_pydict()
        return b''.join(chunks)

class CommonOperators(unittest.TestCase):
    def test_all_100_examples(self):
        self.assertEqual(len(CATALOG),100)
        for name,spec in CATALOG.items():
            with self.subTest(operator=name):self.assertEqual(run(name,spec['sample'],spec['params']),spec['expected'])
    def test_reject_parameters_and_oversized_documents(self):
        with self.assertRaises(ValueError):run('text_uppercase',b'abc',{'typo':1})
        with self.assertRaises(ValueError):run('text_uppercase',b'abc',{'max_input_bytes':2})
        with self.assertRaises(ValueError):run('text_head_lines',b'abc',{'n':-1})
    def test_chunk_boundaries_unicode_and_empty(self):
        self.assertEqual(run('text_uppercase','é世界'.encode(),{}),'É世界'.encode())
        self.assertEqual(run('text_uppercase',b'',{}),b'')
        self.assertEqual(run('csv_unique',b'a,b\n"x,y",1\n"x,y",1\n',{}),b'a,b\n"x,y",1\n')

if __name__=='__main__':unittest.main()

#!/usr/bin/env python3
"""Metadata inference must not execute business processing or need an input directory."""
import io
import json
from pathlib import Path
import sys
import tempfile
import struct
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'dacp-operator-sdk/python'))
import pyarrow as pa
from dacp_operator import Operator, BytesOperator, RecordsOperator, descriptor, result, run_inference, records_schema

class InferenceTest(unittest.TestCase):
    def test_bounded_operator_samples_are_opt_in(self):
        from dacp_operator.inference import evaluate_inference, sample_records, sample_bytes, bounded_sample
        limits={'max_rows':2,'max_bytes':256}
        sample=bounded_sample(sample_records([{'text':'气象'}]*8),limits)
        self.assertLessEqual(len(sample['rows']),2)
        self.assertTrue(sample['truncated'])
        self.assertLessEqual(len(json.dumps(sample).encode()),256)
        huge=bounded_sample(sample_records([{'text':'气'*1000}]),limits)
        self.assertEqual(huge['rows'],[])
        self.assertTrue(huge['truncated'])
        binary=bounded_sample(sample_bytes(b'x'*1000),limits)
        self.assertLessEqual(len(json.dumps(binary).encode()),256)
        self.assertTrue(binary['truncated'])
        calls=[]
        def method(inputs,params):return result({'result':descriptor(['bytes'],['text'],certainty='known')})
        def preview(inputs,params,budget):
            calls.append(budget)
            return {'result':sample_bytes(b'x'*1000,format='text')}
        request={'inputs':{},'params':{}}
        plain=json.loads(evaluate_inference(method,json.dumps(request).encode(),preview))
        self.assertNotIn('sample',plain['outputs']['result'])
        request['preview']=limits
        answer=json.loads(evaluate_inference(method,json.dumps(request).encode(),preview))
        self.assertEqual(calls,[limits])
        self.assertLessEqual(len(json.dumps(answer['outputs']['result']['sample']).encode()),256)
        self.assertTrue(answer['outputs']['result']['sample']['truncated'])
        def bad_preview(*args):raise ValueError('No sample available')
        answer=json.loads(evaluate_inference(method,json.dumps(request).encode(),bad_preview))
        self.assertEqual(answer['outputs']['result']['certainty'],'known')
        self.assertEqual(answer['diagnostics'][0]['code'],'sample_unavailable')

    def test_bytes_and_records_templates(self):
        schema=pa.schema([('value',pa.int64())])
        self.assertEqual(BytesOperator().infer_outputs({'source':descriptor(['bytes'])},{})['outputs']['result']['certainty'],'known')
        answer=RecordsOperator().infer_outputs({'source':descriptor(['records'],schema=schema,certainty='known')},{})
        self.assertEqual(records_schema(answer['outputs']['result']),schema)
        self.assertEqual(RecordsOperator().infer_outputs({'source':descriptor(['records'])},{})['outputs']['result']['certainty'],'unknown')
    def test_method_required_and_empty_outputs_allowed(self):
        with self.assertRaises(NotImplementedError):Operator().infer_outputs({}, {})
        self.assertEqual(result({}),{'outputs':{},'diagnostics':[]})
    def test_cli_never_invokes_processing(self):
        with tempfile.TemporaryDirectory() as tmp:
            request=Path(tmp)/'request.json';request.write_text(json.dumps({'inputs':{},'params':{'n':123}}))
            out=io.BytesIO()
            def method(inputs,params):
                self.assertEqual(params['n'],123)
                print('diagnostic')
                return result({'result':descriptor(['bytes'],['text'],certainty='known')})
            run_inference(method,['infer',str(request)],out,preview=lambda *_: {})
            self.assertEqual(json.loads(out.getvalue())['outputs']['result']['formats'],['text'])
    def test_invalid_result_and_unknown_field(self):
        with self.assertRaises(ValueError):descriptor(['records'],certainty='known')
        schema=pa.schema([('value',pa.int64())])
        class Project(RecordsOperator):
            def output_schema(self,schema,params):
                return pa.schema([schema.field(params['column'])])
        with self.assertRaises(KeyError):Project().infer_outputs({'source':descriptor(['records'],schema=schema,certainty='known')},{'column':'missing'})
    def test_worker_reuses_callable_for_different_requests(self):
        from dacp_operator.inference import serve_inference
        requests = [json.dumps({'inputs': {}, 'params': {'format': f}}).encode() for f in ('json', 'text')]
        source = io.BytesIO(b''.join(struct.pack('>I', len(raw)) + raw for raw in requests))
        output = io.BytesIO(); calls = []
        def method(inputs, params):
            calls.append(params['format'])
            return result({'result': descriptor(['bytes'], [params['format']], certainty='known')})
        serve_inference(method, ['infer-worker', '-', '-'], source, output, preview=lambda *_: {})
        output.seek(0); self.assertEqual(output.read(4), b'\0' * 4)
        for fmt in ('json', 'text'):
            n, = struct.unpack('>I', output.read(4))
            self.assertEqual(json.loads(output.read(n))['outputs']['result']['formats'], [fmt])
        self.assertEqual(calls, ['json', 'text'])
    def test_worker_rejects_partial_and_oversized_frames(self):
        from dacp_operator.inference import serve_inference, MAX_BYTES
        for raw in (b'\0\0', struct.pack('>I', MAX_BYTES + 1), struct.pack('>I', 2) + b'{'):
            with self.assertRaises(ValueError):
                serve_inference(lambda *_: self.fail('invalid frame'), ['infer-worker', '-', '-'], io.BytesIO(raw), io.BytesIO(), preview=lambda *_: {})
if __name__=='__main__':unittest.main()

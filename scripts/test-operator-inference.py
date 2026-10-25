#!/usr/bin/env python3
"""Metadata inference must not execute business processing or need an input directory."""
import io
import json
from pathlib import Path
import sys
import tempfile
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'dacp-operator-sdk/python'))
import pyarrow as pa
from dacp_operator import Operator, BytesOperator, RecordsOperator, descriptor, result, run_inference, records_schema

class InferenceTest(unittest.TestCase):
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
            run_inference(method,['infer',str(request)],out)
            self.assertEqual(json.loads(out.getvalue())['outputs']['result']['formats'],['text'])
    def test_invalid_result_and_unknown_field(self):
        with self.assertRaises(ValueError):descriptor(['records'],certainty='known')
        schema=pa.schema([('value',pa.int64())])
        class Project(RecordsOperator):
            def output_schema(self,schema,params):
                return pa.schema([schema.field(params['column'])])
        with self.assertRaises(KeyError):Project().infer_outputs({'source':descriptor(['records'],schema=schema,certainty='known')},{'column':'missing'})
if __name__=='__main__':unittest.main()

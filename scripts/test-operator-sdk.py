#!/usr/bin/env python3
"""Exercise SDK framing, IPC, template parameters and failure boundaries."""
import io
import json
from pathlib import Path
import struct
import sys
import tempfile
import unittest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dacp-operator-sdk/python'))
import pyarrow as pa
from dacp_operator import OperatorContext, BytesOperator, RecordsOperator


def frames(data):
    result = []
    while data:
        h, n = struct.unpack('>II', data[:8])
        result.append((json.loads(data[8:8+h]), data[8+h:8+h+n]))
        data = data[8+h+n:]
    return result


class SDKTest(unittest.TestCase):
    def test_zero_input_zero_output_operator(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'manifest.json').write_text('[]')
            (directory / 'params.json').write_text('{}')
            output = io.BytesIO()
            with OperatorContext(['operator', tmp, str(directory / 'params.json')], output):
                pass
            self.assertEqual(output.getvalue(), b'')

    def test_templates_and_parameters(self):
        for records in (False, True):
            with tempfile.TemporaryDirectory() as tmp:
                directory = Path(tmp)
                schema = pa.schema([('value', pa.int64())]) if records else pa.schema([('data', pa.binary())])
                batch = pa.record_batch([[1, 2] if records else [b'abc', b'def']], schema=schema)
                with pa.OSFile(str(directory / '0.arrow'), 'wb') as raw:
                    with pa.ipc.new_stream(raw, schema) as writer:
                        writer.write_batch(batch)
                (directory / 'manifest.json').write_text(json.dumps([dict(port='source', file='0.arrow', representation='records' if records else 'bytes', format='arrow')]))
                (directory / 'params.json').write_text('{"upper":true}')
                class Binary(BytesOperator):
                    def transform(self, chunk, params):
                        return chunk.upper() if params['upper'] else chunk
                class Table(RecordsOperator):
                    def transform(self, batch, params):
                        return batch
                output = io.BytesIO()
                # main uses real argv/stdout; preserve both after the invocation.
                old_args, old_stdout = sys.argv, sys.stdout
                class Stdout(io.StringIO):
                    buffer = output
                try:
                    sys.argv = ['example', 'operator', tmp, str(directory / 'params.json')]
                    sys.stdout = Stdout()
                    (Table() if records else Binary()).main()
                finally:
                    sys.argv, sys.stdout = old_args, old_stdout
                emitted = frames(output.getvalue())
                self.assertEqual(emitted[0][0]['type'], 'start')
                self.assertEqual(emitted[-1][0]['type'], 'end')
                payloads = [p for h, p in emitted if h['type'] == 'data']
                if records:
                    self.assertEqual(pa.ipc.open_stream(payloads[0]).read_all().to_pydict(), {'value': [1, 2]})
                else:
                    self.assertEqual(b''.join(payloads), b'ABCDEF')

    def test_failure_has_no_end(self):
        with tempfile.TemporaryDirectory() as tmp:
            directory = Path(tmp)
            (directory / 'manifest.json').write_text('[]')
            (directory / 'params.json').write_text('{}')
            output = io.BytesIO()
            with self.assertRaises(RuntimeError):
                with OperatorContext(['operator', tmp, str(directory / 'params.json')], output) as ctx:
                    ctx.bytes_output('result')
                    raise RuntimeError('business failure')
            self.assertEqual([h['type'] for h, _ in frames(output.getvalue())], ['start'])


if __name__ == '__main__':
    unittest.main()

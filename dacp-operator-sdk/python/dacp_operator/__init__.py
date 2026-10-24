"""Helpers for packaged DACP operators. stdout is reserved for protocol frames."""
import base64
from contextlib import ExitStack, redirect_stdout
import json
from pathlib import Path
import re
import struct
import sys

import pyarrow as pa


class OperatorContext:
    def __init__(self, args=None, output=None, input_streams=None, emit=None):
        args = sys.argv[1:] if args is None else args
        if len(args) != 3 or args[0] != 'operator':
            raise ValueError('expected operator INPUT PARAMS')
        self.directory = Path(args[1])
        self.params = json.loads(Path(args[2]).read_text(encoding='utf-8'))
        if not isinstance(self.params, dict):
            raise ValueError('params must be a JSON object')
        manifest = json.loads((self.directory / 'manifest.json').read_text(encoding='utf-8'))
        if not isinstance(manifest, list):
            raise ValueError('manifest must be an array')
        self._entries = {}
        for entry in manifest:
            name = entry['port']
            if not _valid_name(name) or name in self._entries:
                raise ValueError('invalid or duplicate input port')
            self._entries[name] = entry
        self.input_names = tuple(sorted(self._entries))
        self._index = 0
        self._inputs = {}
        self._outputs = {}
        self._writing = False
        self._closed = False
        self._finished = False
        self._resources = ExitStack()
        self._input_streams = input_streams or {}
        self._emit = emit
        self._output = sys.stdout.buffer if output is None else output
        self._redirect = redirect_stdout(sys.stderr) if emit is None else None

    def __enter__(self):
        self._active()
        if self._redirect is not None:
            self._redirect.__enter__()
        return self

    def __exit__(self, kind, value, traceback):
        try:
            if kind is None:
                if self._index != len(self.input_names):
                    raise ValueError('all inputs must be consumed to Arrow EOS')
                for port in self._outputs:
                    self._frame(port, 'end')
                self._finished = True
        finally:
            try:
                self._resources.close()
            finally:
                self._closed = True
                if self._redirect is not None:
                    self._redirect.__exit__(kind, value, traceback)

    def _active(self):
        if self._closed or self._finished:
            raise ValueError('operator context ended')

    def _frame(self, port, kind, payload=b'', **metadata):
        self._active()
        event = dict(type=kind, port=port, **metadata)
        header = json.dumps(event).encode('utf-8')
        if len(header) > 65536 or len(payload) > 8 * 1024 * 1024:
            raise ValueError('frame exceeds limits; split large record batches')
        if self._emit is not None:
            self._emit(event, payload)
            return
        self._output.write(struct.pack('>II', len(header), len(payload)) + header + payload)
        self._output.flush()

    def input(self, name):
        self._active()
        if name not in self._entries:
            raise ValueError('unknown input port')
        if name not in self._inputs:
            self._inputs[name] = _Input(self, name, self._entries[name])
        return self._inputs[name]

    def _new_output(self, name, representation, format, schema=None):
        self._active()
        if not _valid_name(name) or name in self._outputs:
            raise ValueError('invalid or duplicate output port')
        if self._writing:
            raise ValueError('declare all outputs before writing data')
        metadata = dict(representation=representation, format=format)
        if schema is not None:
            metadata['schema'] = base64.b64encode(schema.serialize().to_pybytes()).decode('ascii')
        self._frame(name, 'start', **metadata)
        value = _Output(self, name, representation, schema)
        self._outputs[name] = value
        return value

    def bytes_output(self, name, format='binary'):
        return self._new_output(name, 'bytes', format)

    def records_output(self, name, schema, format='arrow'):
        return self._new_output(name, 'records', format, schema)


class _Input:
    def __init__(self, context, name, entry):
        self._context, self.name, self._entry = context, name, entry
        self.representation, self.format = entry['representation'], entry['format']
        if self.representation not in ('bytes', 'records'):
            raise ValueError('unsupported input representation')
        self._reader = None
        self._consumed = False
        self._iterating = False

    def _open(self):
        ctx = self._context
        ctx._active()
        if (self._consumed or self._iterating or ctx._index >= len(ctx.input_names)
                or ctx.input_names[ctx._index] != self.name):
            raise ValueError('consume inputs once in lexical port order')
        if self._reader is None:
            filename = self._entry['file']
            if not filename or Path(filename).name != filename or filename in ('.', '..'):
                raise ValueError('invalid input filename')
            self._raw = ctx._resources.enter_context(ctx._input_streams[self.name] if self.name in ctx._input_streams else (ctx.directory / filename).open('rb'))
            self._reader = ctx._resources.enter_context(pa.ipc.open_stream(self._raw))
            if self.representation == 'bytes' and self._reader.schema != pa.schema([('data', pa.binary())]):
                raise ValueError('invalid bytes schema')
        return self._reader

    @property
    def schema(self):
        return self._open().schema

    def _batches(self):
        reader = self._open()
        self._iterating = True
        try:
            yield from reader
            self._consumed = True
            self._context._index += 1
        finally:
            reader.close()
            self._raw.close()

    def records(self):
        if self.representation != 'records':
            raise ValueError('records input required')
        return self._batches()

    def bytes(self):
        if self.representation != 'bytes':
            raise ValueError('bytes input required')
        for batch in self._batches():
            for value in batch.column(0):
                chunk = value.as_py()
                if chunk is None:
                    raise ValueError('null bytes chunk')
                yield chunk


class _Output:
    def __init__(self, context, name, representation, schema):
        self._context, self.name = context, name
        self.representation, self.schema = representation, schema

    def write(self, value):
        ctx = self._context
        ctx._active()
        if self.representation == 'bytes':
            if not isinstance(value, (bytes, bytearray, memoryview)):
                raise TypeError('bytes output requires bytes-like values')
            ctx._writing = True
            for offset in range(0, len(value), 65536):
                ctx._frame(self.name, 'data', bytes(value[offset:offset + 65536]))
        else:
            if not isinstance(value, pa.RecordBatch) or not value.schema.equals(self.schema, check_metadata=True):
                raise ValueError('output schema mismatch')
            ctx._writing = True
            sink = pa.BufferOutputStream()
            with pa.ipc.new_stream(sink, self.schema) as writer:
                writer.write_batch(value)
            ctx._frame(self.name, 'data', sink.getvalue().to_pybytes())


def _valid_name(name):
    return isinstance(name, str) and re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]{0,127}', name) is not None


class BytesOperator:
    output_format = 'binary'

    def transform(self, chunk, params):
        raise NotImplementedError

    def process(self, ctx):
        output = ctx.bytes_output('result', self.output_format)
        for chunk in ctx.input('source').bytes():
            output.write(self.transform(chunk, ctx.params))

    def main(self):
        with OperatorContext() as ctx:
            self.process(ctx)


class RecordsOperator:
    def output_schema(self, input_schema, params):
        return input_schema

    def transform(self, batch, params):
        raise NotImplementedError

    def process(self, ctx):
        source = ctx.input('source')
        output = ctx.records_output('result', self.output_schema(source.schema, ctx.params))
        for batch in source.records():
            result = self.transform(batch, ctx.params)
            if result is not None:
                output.write(result)

    def main(self):
        with OperatorContext() as ctx:
            self.process(ctx)

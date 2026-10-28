"""Structure inference and bounded, operator-supplied output previews."""
import base64
from contextlib import redirect_stdout
import json
from pathlib import Path
import re
import sys
import pyarrow as pa

MAX_BYTES = 1024 * 1024


def descriptor(representations=(), formats=(), schema=None, certainty='unknown', structure=None, sample=None):
    value = dict(representations=list(representations), formats=list(formats), certainty=certainty)
    if schema is not None:
        value['schema'] = base64.b64encode(schema.serialize().to_pybytes()).decode('ascii')
    if structure is not None:
        value['structure'] = structure
    if sample is not None:
        value['sample'] = sample
    _descriptor(value)
    return value


def records_schema(value):
    raw = value.get('schema')
    return pa.ipc.read_schema(pa.BufferReader(base64.b64decode(raw, validate=True))) if raw is not None else None


def result(outputs, diagnostics=()):
    value = dict(outputs=outputs, diagnostics=list(diagnostics))
    validate_result(value)
    return value


def _descriptor(value):
    if not isinstance(value, dict) or not {'representations', 'formats', 'certainty'} <= value.keys() or value.keys() - {'representations', 'formats', 'certainty', 'schema', 'structure', 'sample'}:
        raise ValueError('invalid stream descriptor')
    for key in ('representations', 'formats'):
        xs = value[key]
        if not isinstance(xs, list) or not all(isinstance(x, str) for x in xs) or len(set(xs)) != len(xs):
            raise ValueError('descriptor values must be unique strings')
    if any(not re.fullmatch(r'[a-z][a-z0-9_-]*', x) for x in value['representations']):
        raise ValueError('invalid representation')
    if value['certainty'] not in ('known', 'unknown') or (value['certainty'] == 'known' and not value['representations']):
        raise ValueError('invalid inference certainty')
    if value.get('schema') is not None:
        if value['representations'] != ['records']:
            raise ValueError('Arrow schema requires records representation')
        records_schema(value)
    if value['certainty'] == 'known' and value['representations'] == ['records'] and value.get('schema') is None:
        raise ValueError('known records need a schema')
    if value.get('structure') is not None and not isinstance(value['structure'], dict):
        raise ValueError('structure must be an object')
    if value.get('sample') is not None:
        _sample(value['sample'])
        if value['sample']['representation'] not in value['representations']:
            raise ValueError('sample representation must match output')


def _ports(values):
    if not isinstance(values, dict) or len(values) > 128:
        raise ValueError('invalid inference ports')
    for name, value in values.items():
        if not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]{0,127}', name):
            raise ValueError('invalid inference port')
        _descriptor(value)


def validate_result(value):
    if not isinstance(value, dict) or value.keys() != {'outputs', 'diagnostics'}:
        raise ValueError('inference result requires outputs and diagnostics')
    _ports(value['outputs'])
    ds = value['diagnostics']
    if not isinstance(ds, list) or len(ds) > 128:
        raise ValueError('invalid inference diagnostics')
    for d in ds:
        if not isinstance(d, dict) or d.keys() != {'severity', 'code', 'message'} or d['severity'] not in ('error', 'warning') or not all(isinstance(d[k], str) and d[k] for k in ('code', 'message')):
            raise ValueError('invalid inference diagnostic')


def preview_limits(value):
    if not isinstance(value, dict) or value.keys() != {'max_rows', 'max_bytes'}:
        raise ValueError('invalid preview limits')
    if type(value['max_rows']) is not int or not 1 <= value['max_rows'] <= 100:
        raise ValueError('preview max_rows must be between 1 and 100')
    if type(value['max_bytes']) is not int or not 256 <= value['max_bytes'] <= 65536:
        raise ValueError('preview max_bytes must be between 256 and 65536')
    return value


def sample_records(rows, *, truncated=False):
    return dict(representation='records', format='json', rows=list(rows), truncated=truncated)


def sample_bytes(data, *, format='binary', truncated=False):
    return dict(representation='bytes', format=format,
                data_base64=base64.b64encode(data).decode('ascii'), truncated=truncated)


def _sample(value, max_bytes=65536):
    if not isinstance(value, dict):
        raise ValueError('invalid output sample')
    key = 'rows' if value.get('representation') == 'records' else 'data_base64'
    if value.keys() != {'representation', 'format', 'truncated', key} or not isinstance(value['format'], str) or not 1 <= len(value['format']) <= 128 or type(value['truncated']) is not bool:
        raise ValueError('invalid output sample fields')
    if key == 'rows':
        if not isinstance(value['rows'], list) or len(value['rows']) > 100 or not all(isinstance(row, dict) for row in value['rows']):
            raise ValueError('invalid sample rows')
    elif value['representation'] == 'bytes' and isinstance(value['data_base64'], str):
        base64.b64decode(value['data_base64'], validate=True)
    else:
        raise ValueError('invalid sample representation')
    if len(json.dumps(value, allow_nan=False).encode('utf-8')) > max_bytes:
        raise ValueError('output sample exceeds byte budget')


def bounded_sample(value, limits):
    preview_limits(limits)
    value = dict(value)
    if value.get('representation') == 'records':
        rows = value.get('rows', [])
        value['rows'] = rows[:limits['max_rows']]
        value['truncated'] = bool(value.get('truncated')) or len(rows) > limits['max_rows']
        while len(json.dumps(value, allow_nan=False).encode('utf-8')) > limits['max_bytes'] and value['rows']:
            value['rows'].pop()
            value['truncated'] = True
    elif value.get('representation') == 'bytes':
        data = base64.b64decode(value['data_base64'], validate=True)
        cropped = data[:limits['max_bytes']]
        value['truncated'] = bool(value.get('truncated')) or len(cropped) < len(data)
        while True:
            value['data_base64'] = base64.b64encode(cropped).decode('ascii')
            excess = len(json.dumps(value, allow_nan=False).encode('utf-8')) - limits['max_bytes']
            if excess <= 0 or not cropped:
                break
            cropped = cropped[:max(0, len(cropped) - excess)]
            value['truncated'] = True
    _sample(value, limits['max_bytes'])
    return value


def _attach_samples(value, samples, limits):
    if not isinstance(samples, dict) or not samples.keys() <= value['outputs'].keys():
        raise ValueError('sample refers to an undeclared output port')
    value = {**value, 'outputs': {k: {a: b for a, b in d.items() if a != 'sample'} for k, d in value['outputs'].items()}}
    budget = 65536
    for port, sample in sorted(samples.items()):
        if budget < 256:
            break
        bounded = bounded_sample(sample, {**limits, 'max_bytes': min(limits['max_bytes'], budget)})
        value['outputs'][port]['sample'] = bounded
        budget -= len(json.dumps(bounded, allow_nan=False).encode('utf-8'))
    return value


def evaluate_inference(method, raw, preview=None):
    if not callable(preview):
        raise TypeError('every operator must implement preview_outputs and register it with preview=')
    if len(raw) > MAX_BYTES:
        raise ValueError('inference request too large')
    request = json.loads(raw)
    if not isinstance(request, dict) or not {'inputs', 'params'} <= request.keys() or request.keys() - {'inputs', 'params', 'preview'} or not isinstance(request['params'], dict):
        raise ValueError('invalid inference request')
    _ports(request['inputs'])
    with redirect_stdout(sys.stderr):
        value = method(request['inputs'], request['params'])
        if 'preview' in request:
            limits = preview_limits(request['preview'])
            try:
                samples = preview(request['inputs'], request['params'], limits)
                value = _attach_samples(value, samples, limits)
            except Exception as error:
                value = {**value, 'diagnostics': list(value.get('diagnostics', [])) + [
                    {'severity': 'warning', 'code': 'sample_unavailable', 'message': str(error) or 'Sample unavailable'}]}
    validate_result(value)
    raw = json.dumps(value, allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('inference result too large')
    return raw


def serve_inference(method, args=None, input_stream=None, output=None, preview=None):
    """A worker processes requests serially; errors terminate it instead of reusing state."""
    if not callable(preview):
        raise TypeError('every operator must implement preview_outputs')
    import struct
    from contextlib import ExitStack
    args = sys.argv[1:] if args is None else args
    if len(args) != 3 or args[0] != 'infer-worker':
        raise ValueError('expected infer-worker REQUEST_FIFO RESPONSE_FIFO')
    with ExitStack() as stack:
        source = input_stream if input_stream is not None else stack.enter_context(open(args[1], 'rb', buffering=0))
        target = output if output is not None else stack.enter_context(open(args[2], 'wb', buffering=0))
        def read_exact(size):
            data = bytearray()
            while len(data) < size:
                chunk = source.read(size - len(data))
                if not chunk:
                    if not data:
                        return None
                    raise ValueError('truncated inference frame')
                data.extend(chunk)
            return bytes(data)
        def write_all(raw):
            view = memoryview(raw)
            while view:
                n = target.write(view)
                if not n:
                    raise IOError('inference pipe closed')
                view = view[n:]
        write_all(struct.pack('>I', 0)); target.flush()  # Ready; no result is cached.
        while True:
            header = read_exact(4)
            if header is None:
                return
            size, = struct.unpack('>I', header)
            if not 0 < size <= MAX_BYTES:
                raise ValueError('inference frame exceeds limits')
            raw = read_exact(size)
            if raw is None:
                raise ValueError('truncated inference request')
            response = evaluate_inference(method, raw, preview)
            write_all(struct.pack('>I', len(response))); write_all(response); target.flush()


def run_inference(method, args=None, output=None, preview=None):
    args = sys.argv[1:] if args is None else args
    if args and args[0] == 'infer-worker':
        return serve_inference(method, args, output=output, preview=preview)
    if len(args) != 2 or args[0] != 'infer':
        raise ValueError('expected infer REQUEST_JSON')
    path = Path(args[1])
    if path.stat().st_size > MAX_BYTES:
        raise ValueError('inference request too large')
    (sys.stdout.buffer if output is None else output).write(evaluate_inference(method, path.read_bytes(), preview))


def run_operator(process, infer_outputs, args=None, preview_outputs=None):
    """Entrypoint for function-style operators. Both methods are required."""
    from . import OperatorContext
    if not callable(preview_outputs):
        raise TypeError('every operator must implement preview_outputs')
    args = sys.argv[1:] if args is None else args
    if args and args[0] in ('infer', 'infer-worker'):
        run_inference(infer_outputs, args, preview=preview_outputs)
    else:
        with OperatorContext(args) as ctx:
            process(ctx)


def preview_process(process, inputs, params, limits):
    """Run explicitly opted-in pure transforms on bounded in-memory samples only.

    No filesystem paths or remote streams are exposed. This is not a sandbox for
    arbitrary user code; use it only for side-effect-free processing methods.
    """
    from types import SimpleNamespace
    import datetime
    import decimal
    limits = preview_limits(limits)
    outputs = {}
    class EnoughSamples(BaseException):
        pass
    def json_value(v):
        if isinstance(v, (datetime.date, datetime.time, datetime.datetime)):
            return v.isoformat()
        if isinstance(v, decimal.Decimal):
            return str(v)
        if isinstance(v, bytes):
            return base64.b64encode(v).decode('ascii')
        if isinstance(v, dict):
            return {k: json_value(x) for k, x in v.items()}
        if isinstance(v, (list, tuple)):
            return [json_value(x) for x in v]
        return v
    class Source:
        def __init__(self, name):
            self.description = inputs[name]
            sample = self.description.get('sample')
            if sample is None:
                raise ValueError('Upstream sample unavailable for input: ' + name)
            self.sample = bounded_sample(sample, limits)
            self.schema = records_schema(self.description)
            if self.schema is None and sample['representation'] == 'records':
                # A sample-only schema is not advertised as the inferred output schema.
                self.schema = pa.Table.from_pylist(self.sample['rows']).schema
            self.representation = sample['representation']
            self.format = sample['format']
        def bytes(self):
            if self.representation != 'bytes':
                raise ValueError('bytes sample required')
            yield base64.b64decode(self.sample['data_base64'], validate=True)
        def records(self):
            if self.representation != 'records' or self.schema is None:
                raise ValueError('record sample and schema required')
            yield pa.RecordBatch.from_pylist(self.sample['rows'], schema=self.schema)
    def records_output(port, schema):
        sample = sample_records([])
        outputs[port] = sample
        def write(batch):
            for row in batch.to_pylist():
                if len(sample['rows']) >= limits['max_rows']:
                    sample['truncated'] = True
                    raise EnoughSamples()
                candidate = bounded_sample({**sample, 'rows': sample['rows'] + [json_value(row)]}, limits)
                if len(candidate['rows']) <= len(sample['rows']):
                    sample['truncated'] = True
                    raise EnoughSamples()
                sample.update(candidate)
        return SimpleNamespace(write=write)
    def bytes_output(port, format='binary'):
        outputs[port] = sample_bytes(b'', format=format)
        data = bytearray()
        def write(chunk):
            data.extend(bytes(chunk)[:max(0, limits['max_bytes'] + 1 - len(data))])
            outputs[port] = bounded_sample(sample_bytes(bytes(data), format=format), limits)
            if outputs[port]['truncated']:
                raise EnoughSamples()
        return SimpleNamespace(write=write)
    context = SimpleNamespace(params=params, input=Source, input_names=list(inputs),
                              records_output=records_output, bytes_output=bytes_output)
    try:
        process(context)
    except EnoughSamples:
        pass
    if any(d.get('sample', {}).get('truncated') for d in inputs.values()):
        for sample in outputs.values():
            sample['truncated'] = True
    return {port: bounded_sample(sample, limits) for port, sample in outputs.items()}

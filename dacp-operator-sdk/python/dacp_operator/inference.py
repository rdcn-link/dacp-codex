"""Mandatory metadata-only inference protocol. No input streams are opened."""
import base64
from contextlib import redirect_stdout
import json
from pathlib import Path
import re
import sys
import pyarrow as pa

MAX_BYTES = 1024 * 1024


def descriptor(representations=(), formats=(), schema=None, certainty='unknown', structure=None):
    value = dict(representations=list(representations), formats=list(formats), certainty=certainty)
    if schema is not None:
        value['schema'] = base64.b64encode(schema.serialize().to_pybytes()).decode('ascii')
    if structure is not None:
        value['structure'] = structure
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
    if not isinstance(value, dict) or not {'representations', 'formats', 'certainty'} <= value.keys() or value.keys() - {'representations', 'formats', 'certainty', 'schema', 'structure'}:
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


def run_inference(method, args=None, output=None):
    args = sys.argv[1:] if args is None else args
    if len(args) != 2 or args[0] != 'infer':
        raise ValueError('expected infer REQUEST_JSON')
    path = Path(args[1])
    if path.stat().st_size > MAX_BYTES:
        raise ValueError('inference request too large')
    request = json.loads(path.read_bytes())
    if not isinstance(request, dict) or request.keys() != {'inputs', 'params'} or not isinstance(request['params'], dict):
        raise ValueError('invalid inference request')
    _ports(request['inputs'])
    with redirect_stdout(sys.stderr):
        value = method(request['inputs'], request['params'])
    validate_result(value)
    raw = json.dumps(value, allow_nan=False).encode()
    if len(raw) > MAX_BYTES:
        raise ValueError('inference result too large')
    (sys.stdout.buffer if output is None else output).write(raw)


def run_operator(process, infer_outputs, args=None):
    """Entrypoint for function-style operators. Both methods are required."""
    from . import OperatorContext
    args = sys.argv[1:] if args is None else args
    if args and args[0] == 'infer':
        run_inference(infer_outputs, args)
    else:
        with OperatorContext(args) as ctx:
            process(ctx)

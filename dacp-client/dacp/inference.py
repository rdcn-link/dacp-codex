"""Metadata-only structure inference over Flight, sharing the ordinary connection helper."""
import json
from urllib.parse import urlsplit
import pyarrow.flight as flight
from .source import _flight_client


def infer(node, body, *, workflow=False, tls_root_certs=None, cert_chain=None, private_key=None, token=None, timeout=120):
    u = urlsplit(node)
    if u.scheme != 'dacp' or not u.hostname or u.username or u.password or u.path not in ('', '/') or u.query or u.fragment:
        raise ValueError('expected DACP origin')
    if timeout <= 0:
        raise ValueError('timeout must be positive')
    raw = json.dumps(body, allow_nan=False).encode()
    if len(raw) > 1024 * 1024:
        raise ValueError('inference request exceeds 1 MiB')
    client = _flight_client(u, tls_root_certs, cert_chain, private_key, **({"token": token} if token is not None else {}))
    try:
        return infer_client(client, raw, workflow=workflow, timeout=timeout)
    finally:
        client.close()


def infer_client(client, raw, *, workflow=False, timeout=120):
    if len(raw) > 1024 * 1024:
        raise ValueError('inference request exceeds 1 MiB')
    values = iter(client.do_action(flight.Action('dacp.infer_job' if workflow else 'dacp.infer', raw),
                                  options=flight.FlightCallOptions(timeout=timeout)))
    first = next(values, None)
    if first is None or next(values, None) is not None or len(first.body) > 1024 * 1024:
        raise ValueError('invalid inference response')
    result = json.loads(first.body.to_pybytes())
    if not isinstance(result, dict):
        raise ValueError('invalid inference response')
    return result

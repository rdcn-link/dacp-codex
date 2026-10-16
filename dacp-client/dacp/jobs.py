"""Synchronous job submission over Flight; failures are never retried automatically."""
from urllib.parse import urlsplit
import pyarrow.flight as flight
from . import dacp_pb2 as pb
from .source import _flight_client

class Job:
    @staticmethod
    def run(node, yaml, *, tls_root_certs=None, cert_chain=None, private_key=None, timeout=3600):
        u = urlsplit(node)
        if (u.scheme != "dacp" or not u.hostname or u.username or u.password
                or u.path not in ("", "/") or u.query or u.fragment):
            raise ValueError("expected node origin dacp://host[:port]")
        body = pb.RunJobRequest(yaml=yaml).SerializeToString()
        if len(body) > 1024 * 1024:
            raise ValueError("control request exceeds 1 MiB")
        client = _flight_client(u, tls_root_certs, cert_chain, private_key)
        try:
            results = iter(client.do_action(flight.Action("dacp.run_job", body),
                                           options=flight.FlightCallOptions(timeout=timeout)))
            first = next(results, None)
            if first is None:
                raise RuntimeError("coordinator returned no job result")
            result = pb.RunJobResult.FromString(first.body.to_pybytes())
            if next(results, None) is not None:
                raise RuntimeError("coordinator returned multiple job results")
            return dict(result.outputs)
        finally:
            client.close()

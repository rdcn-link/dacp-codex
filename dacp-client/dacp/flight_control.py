"""Shared Flight action/ticket transport for user and collaboration SDKs."""
import json
import pyarrow.flight as flight

class FlightControl:
    def _action(self, name, body=None):
        raw = json.dumps(body or {}).encode()
        if len(raw) > 1024*1024:
            raise ValueError('Union request exceeds 1 MiB')
        responses = iter(self.client.do_action(flight.Action('dacp.union.' + name, raw), flight.FlightCallOptions(timeout=self.timeout)))
        response = next(responses, None)
        if response is None or len(response.body) > 1024*1024 or next(responses, None) is not None:
            raise ValueError('invalid Union response')
        return json.loads(response.body.to_pybytes())

    def _stream(self, kind, body):
        raw = json.dumps(dict(body, type='dacp.union.' + kind)).encode()
        if len(raw) > 1024*1024:
            raise ValueError('Union ticket exceeds 1 MiB')
        return self.client.do_get(flight.Ticket(raw), flight.FlightCallOptions(timeout=self.timeout))


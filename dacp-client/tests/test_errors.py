import io
import traceback
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.flight as flight
import pytest

from dacp import DacpError
from dacp.errors import translate
from dacp.source import DataSet, Stream
from dacp import cli, dacp_pb2 as pb


@pytest.mark.parametrize('error,code,message', [
    (pa.ArrowKeyError('NOT_FOUND: dataset not found; private detail'), 'NOT_FOUND', 'Dataset not found.'),
    (flight.FlightUnauthorizedError('FORBIDDEN: grpc private/path'), 'ACCESS_DENIED', 'Access denied.'),
    (flight.FlightUnavailableError('grpc+tls://private-host'), 'UNAVAILABLE', 'Cannot reach'),
    (flight.FlightTimedOutError('debug_error_string private'), 'TIMEOUT', 'timed out'),
    (pa.ArrowKeyError('NOT_FOUND: logical resource unavailable'), 'NOT_FOUND', 'Item or dataset not found.'),
    (flight.FlightServerError('EXPIRED: private output'), 'EXPIRED', 'expired'),
    (flight.FlightServerError('SQL_ERROR: driver private table'), 'REQUEST_FAILED', 'request failed'),
    (flight.FlightError('private unrecognized failure'), 'REQUEST_FAILED', 'request failed'),
])
def test_public_error_messages_never_echo_diagnostics(error, code, message):
    result = translate(error)
    assert result.code == code
    assert message in str(result)
    assert 'private' not in str(result) and 'grpc' not in str(result)
    assert result.__cause__ is None


@pytest.mark.parametrize('operation', ['list', 'count', 'open'])
def test_sdk_metadata_failures_are_dacp_errors(operation):
    class Client:
        def get_flight_info(self, descriptor):
            raise pa.ArrowKeyError('NOT_FOUND: dataset not found; secret-driver')
    ds = DataSet(Client(), 'missing', 'dacp://host/missing')
    with pytest.raises(DacpError) as caught:
        getattr(ds, operation)(*(() if operation == 'list' else ('/orders',)))
    assert str(caught.value) == 'Dataset not found. Check the dataset name.'
    assert 'secret-driver' not in ''.join(traceback.format_exception(caught.value))


@pytest.mark.parametrize('view', ['records', 'bytes'])
def test_stream_failure_is_sanitized_and_cancelled(view):
    class Reader:
        cancelled = False
        def __iter__(self):
            raise flight.FlightServerError('private database driver')
            yield
        def cancel(self): self.cancelled = True
    reader = Reader()
    client = SimpleNamespace(do_get=lambda ticket: reader)
    ticket = flight.Ticket(pb.Ticket(dataset='public', item='/orders').SerializeToString())
    stream = Stream(client, ticket)
    with pytest.raises(DacpError):
        if view == 'records':
            list(stream.as_records())
        else:
            with stream.as_bytes() as data:
                data.read()
    assert reader.cancelled


def test_shell_hides_transport_and_unknown_errors(monkeypatch, capsys):
    class Dataset:
        def list(self):
            raise flight.FlightUnavailableError('grpc private server')
        def close(self): pass
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: Dataset())
    assert cli.main(['dacp://host/public']) == 1
    output = capsys.readouterr().out
    assert output.startswith('Error: Cannot reach the DACP server.')
    assert 'grpc' not in output and 'private' not in output

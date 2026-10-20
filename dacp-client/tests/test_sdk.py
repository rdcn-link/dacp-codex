import io
import pytest
import pyarrow as pa
from dacp.source import _Bytes, Source
from dacp import dacp_pb2 as pb

class Reader:
    def __init__(self, chunks): self.chunks, self.cancelled = iter(chunks), False
    def __iter__(self): return self.chunks
    def cancel(self): self.cancelled = True
class Chunk:
    def __init__(self, values): self.data = pa.record_batch([pa.array(values, type=pa.binary())], names=["data"])

def test_bytes_partial_and_eof():
    r = Reader([Chunk([b"ab", b"cd"]), Chunk([b"ef"])])
    with io.BufferedReader(_Bytes(r), buffer_size=2) as stream:
        assert stream.read(3) == b"abc"
        assert stream.read() == b"def"
        assert stream.read() == b""
    assert r.cancelled

def test_protocol_roundtrip():
    ticket = pb.Ticket(dataset="data1", item="a/b.csv", view="bytes")
    assert pb.Ticket.FromString(ticket.SerializeToString()) == ticket

@pytest.mark.parametrize("uri", ["https://host/d", "dacp://host/../d", "dacp://host", "dacp://host/d?token=x"])
def test_invalid_uri(uri):
    with pytest.raises(ValueError): Source.open(uri)

def test_failed_byte_stream_is_terminal():
    def broken():
        yield Chunk([b"ok"])
        raise RuntimeError("transport failed")
    r = Reader(broken())
    stream = _Bytes(r)
    with pytest.raises(RuntimeError): stream.read()
    assert r.cancelled
    with pytest.raises(OSError, match="reopen"): stream.read()
    stream.close()

class MetadataClient:
    def __init__(self): self.descriptors = []
    def get_flight_info(self, descriptor):
        self.descriptors.append([p.decode() for p in descriptor.path])
        item = pb.Item(path='/reports/orders', format='csv', kind='data', views=['records', 'bytes'])
        if len(descriptor.path) == 3:
            return type('Info', (), {'schema': pa.schema([('id', pa.int64())]), 'app_metadata': item.SerializeToString(),
                'endpoints': [type('Endpoint', (), {'ticket': object()})()]})()
        folder = pb.Item(path='/reports', kind='folder')
        return type('Info', (), {'app_metadata': pb.DataSet(items=[folder,item]).SerializeToString()})()


def test_logical_listing_descriptors_and_metadata():
    from dacp.source import DataSet
    client = MetadataClient()
    ds = DataSet(client,'public','dacp://host/public')
    assert ds.list()[0].kind == 'folder'
    assert ds.list('/')[1].views == ('records','bytes')
    ds.list('/reports')
    assert client.descriptors == [['datasets','public'], ['datasets','public','list','/'],
                                  ['datasets','public','list','/reports']]
    assert ds.open('/reports/orders').views == ('records','bytes')


@pytest.mark.parametrize('path', ['relative','/../data','/a//b','/a/','/a\\b','/a\x00'])
def test_invalid_logical_paths_fail_before_rpc(path):
    from dacp.source import DataSet
    client = MetadataClient()
    ds = DataSet(client,'public','dacp://host/public')
    with pytest.raises(ValueError): ds.list(path)
    with pytest.raises(ValueError): ds.open(path)
    assert client.descriptors == []


def test_direct_item_uri_becomes_absolute_logical_path(monkeypatch):
    from dacp import source
    client = MetadataClient()
    monkeypatch.setattr(source, '_flight_client', lambda *args: client)
    stream = Source.open('dacp://host/public/reports/orders')
    assert client.descriptors == [['datasets','public','/reports/orders']]
    assert stream._owns_client


def test_item_statistics_preserve_unknown_and_zero(monkeypatch):
    from datetime import datetime, timezone
    from dacp.source import DataSet
    class Client:
        def get_flight_info(self, descriptor):
            meta = pb.DataSet(items=[pb.Item(path='/known', kind='data', last_modified_time_ms=0,
                                             total_size_bytes=0, record_count=0),
                                     pb.Item(path='/unknown', kind='data')])
            return type('Info', (), {'app_metadata': meta.SerializeToString()})()
    known, unknown = DataSet(Client(), 'public', 'dacp://host/public').list()
    assert known.last_modified_time == datetime(1970, 1, 1, tzinfo=timezone.utc)
    assert known.total_size_bytes == 0 and known.record_count == 0
    assert unknown.last_modified_time is None
    assert unknown.total_size_bytes is None and unknown.record_count is None


def test_explicit_count_preserves_zero_and_unknown():
    from dacp.source import DataSet
    class Client:
        def get_flight_info(self, descriptor):
            assert descriptor.path[:3] == [b'datasets', b'public', b'count']
            item = pb.Item(path=descriptor.path[3].decode(), kind='data')
            if item.path == '/empty':
                item.record_count = 0
            return type('Info', (), {'app_metadata': item.SerializeToString()})()
    ds = DataSet(Client(), 'public', 'dacp://host/public')
    assert ds.count('/empty') == 0
    assert ds.count('/image') is None
    with pytest.raises(ValueError):
        ds.count('relative')


@pytest.mark.parametrize('kind,views,readable', [('folder', ['bytes'], True), ('folder', [], False)])
def test_open_uses_advertised_views_for_containers(kind, views, readable):
    from dacp.source import DataSet
    class Client:
        def get_flight_info(self, descriptor):
            metadata = pb.Item(path='/archive', kind=kind, format='tar-gzip', views=views)
            return type('Info', (), {'app_metadata': metadata.SerializeToString(),
                'endpoints': [type('Endpoint', (), {'ticket': object()})()]})()
    ds = DataSet(Client(), 'public', 'dacp://host/public')
    if readable:
        assert ds.open('/archive').views == ('bytes',)
    else:
        with pytest.raises(ValueError, match='no readable views'):
            ds.open('/archive')


def test_dataset_discovery_local_scope_and_cleanup(monkeypatch):
    import dacp.source as source
    from dacp import dacp_pb2 as pb
    calls = []
    class Client:
        closed = False
        def list_flights(self, criteria, options):
            calls.append(criteria)
            if criteria == b'local' and len(calls) == 3:
                raise ValueError('local directory unsupported')
            return [type('Info', (), {'app_metadata': pb.DataSet(name='orders', uri='dacp://node/orders', title='Orders').SerializeToString()})()]
        def close(self):
            self.closed = True
    clients = []
    def connect(*args):
        client = Client()
        clients.append(client)
        return client
    monkeypatch.setattr(source, '_flight_client', connect)
    assert source.Source.datasets('dacp://node')[0].name == 'orders'
    assert source.Source.datasets('dacp://node', local_only=True)[0].title == 'Orders'
    with pytest.raises(ValueError):
        source.Source.datasets('dacp://union', local_only=True)
    assert calls == [b'', b'local', b'local']
    assert all(client.closed for client in clients)

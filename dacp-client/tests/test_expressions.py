from datetime import date, datetime, timezone
from decimal import Decimal
import pytest
import pyarrow as pa
from dacp import col
from dacp.expressions import scan_options
from dacp import dacp_pb2 as pb


def test_typed_tree_and_scan_roundtrip():
    predicate = ((col('n') > Decimal('1.234567890123456789'))
                 & (col('name').isin(['A', 'B']) | col('time').is_null()))
    scan = scan_options(predicate, ['name'], 0)
    copied = pb.Scan.FromString(scan.SerializeToString())
    assert copied.predicate.op == 'and'
    assert copied.predicate.children[0].values[0].value == '1.234567890123456789'
    assert copied.predicate.children[0].values[0].type == 'decimal'
    assert list(copied.projection.columns) == ['name']
    assert copied.HasField('limit') and copied.limit == 0
    assert scan_options() is None


def test_nulls_negation_and_temporal_literals():
    assert (~col('n').is_not_null())._message.children[0].op == 'is_not_null'
    for value, kind in [(date(2026, 1, 1), 'date'),
                        (datetime(2026, 1, 1), 'datetime'),
                        (datetime(2026, 1, 1, tzinfo=timezone.utc), 'timestamp')]:
        assert (col('time') == value)._message.values[0].type == kind
    with pytest.raises(TypeError): bool(col('n') > 1)
    with pytest.raises(ValueError): col('n') == None
    with pytest.raises(ValueError): col('n').isin([1, None])
    with pytest.raises(ValueError): col('n') > float('nan')


@pytest.mark.parametrize('options', [dict(columns=[]), dict(columns=['a', 'a']),
    dict(columns=['a', 2]), dict(limit=-1), dict(limit=True), dict(limit=2**63)])
def test_invalid_options(options):
    with pytest.raises(ValueError): scan_options(**options)


def test_sdk_transports_query_and_rejects_bytes():
    import pyarrow.flight as flight
    from dacp.source import DataSet
    class Client:
        def get_flight_info(self, descriptor):
            metadata = pb.Item(path='/records', views=['records', 'bytes'])
            return type('Info', (), dict(schema=pa.schema([('id', pa.int64())]), app_metadata=metadata.SerializeToString(), endpoints=[
                type('Endpoint', (), dict(ticket=flight.Ticket(pb.Ticket(dataset='data', item='/records').SerializeToString())))()]))()
        def do_get(self, ticket):
            self.ticket = pb.Ticket.FromString(ticket.ticket)
            class Reader:
                def __iter__(self): return iter([])
                def cancel(self): pass
            return Reader()
    client = Client()
    with DataSet(client, 'data', 'dacp://localhost/data').open('/records', predicate=col('id') > 2,
                                                            columns=['name'], limit=3) as stream:
        assert list(stream.as_records()) == []
        assert client.ticket.scan.predicate.column == 'id'
        assert list(client.ticket.scan.projection.columns) == ['name']
        assert client.ticket.scan.limit == 3
        with pytest.raises(ValueError, match='records view'): stream.as_bytes()

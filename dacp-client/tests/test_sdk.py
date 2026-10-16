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

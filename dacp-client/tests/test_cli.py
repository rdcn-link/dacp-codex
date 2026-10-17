import io
from types import SimpleNamespace

import pyarrow as pa
import pytest

from dacp import cli


class FakeStream:
    def __init__(self):
        self.cancelled = False
        self.reader = io.BytesIO(b'hello\x1b[31m world\n')

    def __enter__(self):
        return self

    def __exit__(self, *args):
        pass

    def as_records(self):
        try:
            yield pa.record_batch([list(range(100))], names=['id'])
            raise AssertionError('preview consumed a second batch')
        finally:
            self.cancelled = True

    def as_bytes(self):
        return self.reader


class FakeDataset:
    def __init__(self, broken=False):
        self.closed = False
        self.broken = broken
        self.stream = FakeStream()

    def list(self):
        if self.broken:
            raise RuntimeError('connection refused')
        return [SimpleNamespace(path='orders.csv', format='csv'),
                SimpleNamespace(path='file name.txt', format='txt')]

    def open(self, path):
        self.opened = path
        return self.stream

    def close(self):
        self.closed = True


@pytest.fixture
def browser(monkeypatch):
    ds = FakeDataset()
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: ds)
    output = io.StringIO()
    b = cli.Browser('dacp://localhost/data1', rows=3, byte_limit=32, stdout=output)
    yield b, ds, output
    b.close()


def test_record_preview_is_bounded_and_cancels(browser):
    b, ds, out = browser
    b.onecmd('show 1')
    assert '"id": 2' in out.getvalue()
    assert '"id": 3' not in out.getvalue()
    assert '3 rows shown' in out.getvalue()
    assert ds.stream.cancelled


def test_text_preview_escapes_terminal_controls_and_closes(browser):
    b, ds, out = browser
    b.onecmd('show "file name.txt" text')
    assert ds.opened == 'file name.txt'
    assert '\x1b' not in out.getvalue()
    assert r'\x1b' in out.getvalue()
    assert ds.stream.reader.closed


def test_failed_switch_keeps_existing_connection(browser, monkeypatch):
    b, ds, out = browser
    bad = FakeDataset(broken=True)
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: bad)
    b.onecmd('connect dacp://other/data2')
    assert b.dataset is ds and not ds.closed
    assert bad.closed
    assert 'Error: connection refused' in out.getvalue()


def test_invalid_selection_and_mode_do_not_exit(browser):
    b, ds, out = browser
    b.onecmd('show 0')
    b.onecmd('show 1 unknown')
    assert 'out of range' in out.getvalue()
    assert 'view must be' in out.getvalue()
    assert not ds.closed
    assert b.onecmd('quit')


def test_switch_dataset_encodes_name(browser, monkeypatch):
    b, ds, out = browser
    new = FakeDataset()
    uris = []
    def connect(uri, **kwargs):
        uris.append(uri)
        return new
    monkeypatch.setattr(cli.Source, 'open', connect)
    b.onecmd('use sales data')
    assert uris == ['dacp://localhost/sales%20data']
    assert ds.closed
    assert b.dataset is new


def test_byte_preview_limit(browser):
    b, ds, out = browser
    b.byte_limit = 5
    b.onecmd('show 2 hex')
    assert '68 65 6c 6c 6f' in out.getvalue()
    assert '5 bytes shown' in out.getvalue()
    assert ds.stream.reader.closed


def test_main_closes_connection_at_eof(monkeypatch):
    ds = FakeDataset()
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: ds)
    monkeypatch.setattr('builtins.input', lambda *a: (_ for _ in ()).throw(EOFError()))
    assert cli.main(['dacp://localhost/data1']) == 0
    assert ds.closed


@pytest.mark.parametrize('value', ['0', '-1'])
def test_reject_nonpositive_limits(value):
    with pytest.raises(SystemExit):
        cli.main(['dacp://localhost/data1', '--rows', value])

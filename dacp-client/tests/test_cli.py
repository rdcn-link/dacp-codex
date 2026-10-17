import io
from types import SimpleNamespace

import pyarrow as pa
import pytest

from dacp import cli


class FakeStream:
    views = ("records", "bytes")
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

    def list(self, path=None):
        self.listed_path = path
        if self.broken:
            raise RuntimeError('connection refused')
        return [SimpleNamespace(path='/orders', format='csv', kind='data', views=('records', 'bytes'), total_size_bytes=0, record_count=0, last_modified_time=None),
                SimpleNamespace(path='/file name', format='txt', kind='data', views=('bytes',), total_size_bytes=None, record_count=None, last_modified_time=None)]

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
    assert '| 2  |' in out.getvalue()
    assert '| 3  |' not in out.getvalue()
    assert '3 rows shown' in out.getvalue()
    assert ds.stream.cancelled


def test_text_preview_escapes_terminal_controls_and_closes(browser):
    b, ds, out = browser
    b.onecmd('show "/file name" text')
    assert ds.opened == '/file name'
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
    assert 'Error: The operation failed.' in out.getvalue()


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


def test_folder_browsing_and_data_only_previews(browser):
    b, ds, out = browser
    b.onecmd('ls /reports')
    assert ds.listed_path == '/reports'
    b.items = [SimpleNamespace(path='/reports', kind='folder', format='', views=())]
    b.onecmd('show 1')
    assert 'use ls /reports' in out.getvalue()


def test_listing_table_headers_utc_time_and_unknown_values(browser, monkeypatch):
    from datetime import datetime, timedelta, timezone
    b, ds, out = browser
    items = ds.list()
    items[0].last_modified_time = datetime(2026, 10, 16, 8, 22, 2, tzinfo=timezone(timedelta(hours=2)))
    items[1].path = '/file\nname'
    monkeypatch.setattr(ds, 'list', lambda: items)
    out.seek(0)
    out.truncate()
    b.onecmd('ls')
    lines = out.getvalue().splitlines()
    assert [cell.strip() for cell in lines[1].split('|')[1:-1]] == ['Path', 'Format', 'Bytes', 'Records', 'Modified (UTC)']
    assert [cell.strip() for cell in lines[3].split('|')[1:-1]] == ['/orders', 'csv', '0', '0', '2026-10-16 06:22:02']
    assert [cell.strip() for cell in lines[4].split('|')[1:-1]] == [r'/file\nname', 'txt', '-', '-', '-']
    assert len(lines) == 7


def test_empty_listing_keeps_table_headers(browser, monkeypatch):
    b, ds, out = browser
    monkeypatch.setattr(ds, 'list', lambda: [])
    out.seek(0)
    out.truncate()
    b.onecmd('ls')
    assert 'Modified (UTC)' in out.getvalue()
    assert out.getvalue().endswith('0 items\n')


def test_explicit_count_uses_number_or_path(browser, monkeypatch):
    b, ds, out = browser
    paths = []
    def count(path):
        paths.append(path)
        return 0 if path == '/orders' else None
    monkeypatch.setattr(ds, 'count', count, raising=False)
    b.onecmd('count 1')
    b.onecmd('count "/file name"')
    assert paths == ['/orders', '/file name']
    assert '/orders: 0 records' in out.getvalue()
    assert '/file name: - records' in out.getvalue()


@pytest.mark.parametrize('terminal,no_color', [(True, False), (False, False), (True, True)])
def test_folder_highlighting_preserves_table_and_escapes_paths(browser, monkeypatch, terminal, no_color):
    b, ds, out = browser
    monkeypatch.setattr(out, 'isatty', lambda: terminal)
    monkeypatch.setenv('TERM', 'xterm')
    if no_color:
        monkeypatch.setenv('NO_COLOR', '')
    else:
        monkeypatch.delenv('NO_COLOR', raising=False)
    items = ds.list()
    items[1].kind = 'folder'
    items[1].format = ''
    items[1].path = '/folder\x1b[31m'
    monkeypatch.setattr(ds, 'list', lambda: items)
    out.seek(0)
    out.truncate()
    b.onecmd('ls')
    rendered = out.getvalue()
    assert '\x1b[31m' not in rendered
    assert r'/folder\x1b[31m' in rendered
    assert ('\x1b[1;36m' in rendered) == (terminal and not no_color)
    rows = rendered.splitlines()
    plain = rows[4].replace('\x1b[1;36m', '').replace('\x1b[0m', '')
    assert len(plain) == len(rows[0])
    assert [cell.strip() for cell in plain.split('|')[1:-1]][1:] == ['-', '-', '-', '-']


@pytest.mark.parametrize('command,headers', [('show 1', ['id'])])
def test_show_has_outer_border_and_column_headers(browser, command, headers):
    b, ds, out = browser
    out.seek(0)
    out.truncate()
    b.onecmd(command)
    lines = out.getvalue().splitlines()
    assert lines[0].startswith('+') and lines[0].endswith('+')
    assert [cell.strip() for cell in lines[1].split('|')[1:-1]] == headers
    assert lines[0] == lines[2] == lines[-2]
    assert all(len(line) == len(lines[0]) for line in lines[:-1])


@pytest.mark.parametrize('mode', ['text', 'hex'])
def test_nonrecord_previews_keep_plain_display(browser, mode):
    b, ds, out = browser
    out.seek(0)
    out.truncate()
    b.onecmd('show 2 ' + mode)
    rendered = out.getvalue()
    assert not rendered.startswith('+')
    if mode == 'text':
        assert rendered.startswith(r'hello\x1b[31m world')
    else:
        assert rendered.startswith('00000000  68 65 6c 6c 6f')


def test_auto_preview_uses_advertised_views_for_new_formats(browser):
    b, ds, out = browser
    b.items[0].format = 'excel'
    b.onecmd('show 1')
    assert '3 rows shown' in out.getvalue()


def test_auto_preview_does_not_infer_records_from_filename(browser):
    b, ds, out = browser
    ds.stream.views = ('bytes',)
    b.items[0].path = '/orders.csv'
    b.onecmd('show 1')
    assert '68 65 6c 6c 6f' in out.getvalue()
    assert 'rows shown' not in out.getvalue()


def test_zip_folder_supports_bytes_preview(browser):
    b, ds, out = browser
    b.items = [SimpleNamespace(path='/archive', kind='folder', format='zip', views=('bytes',))]
    ds.stream.views = ('bytes',)
    b.onecmd('show 1 hex')
    assert 'folder has no content' not in out.getvalue()
    assert ds.opened == '/archive'
    assert 'Error:' not in out.getvalue()

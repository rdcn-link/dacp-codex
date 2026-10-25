import io
from types import SimpleNamespace

import pyarrow as pa
import pytest

from dacp import cli


class FakeStream:
    views = ("records", "bytes")
    item_schema = pa.schema([("id", pa.int64())])
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

    def open(self, path, **options):
        self.opened, self.options = path, options
        return self.stream

    def close(self):
        self.closed = True


@pytest.fixture
def browser(monkeypatch):
    ds = FakeDataset()
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: ds)
    output = io.StringIO()
    monkeypatch.setattr(cli.Source, 'datasets', lambda *a, **kw: [])
    b = cli.Browser('dacp://localhost', rows=3, byte_limit=32, stdout=output)
    b.onecmd('use data1')
    yield b, ds, output
    b.close()


def test_record_preview_is_bounded_and_cancels(browser):
    b, ds, out = browser
    b.onecmd('cat 1')
    assert '| 2  |' in out.getvalue()
    assert '| 3  |' not in out.getvalue()
    assert '3 rows shown' in out.getvalue()
    assert ds.stream.cancelled


def test_text_preview_escapes_terminal_controls_and_closes(browser):
    b, ds, out = browser
    b.onecmd('cat "/file name" text')
    assert ds.opened == '/file name'
    assert '\x1b' not in out.getvalue()
    assert r'\x1b' in out.getvalue()
    assert ds.stream.reader.closed


def test_failed_switch_keeps_existing_connection(browser, monkeypatch):
    b, ds, out = browser
    bad = FakeDataset(broken=True)
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: bad)
    b.onecmd('use data2')
    assert b.dataset is ds and not ds.closed
    assert bad.closed
    assert 'Error: The operation failed.' in out.getvalue()


def test_invalid_selection_and_mode_do_not_exit(browser):
    b, ds, out = browser
    b.onecmd('cat 0')
    b.onecmd('cat 1 unknown')
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
    b.onecmd('cat 2 hex')
    assert '68 65 6c 6c 6f' in out.getvalue()
    assert '5 bytes shown' in out.getvalue()
    assert ds.stream.reader.closed


def test_main_closes_connection_at_eof(monkeypatch):
    ds = FakeDataset()
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: ds)
    monkeypatch.setattr(cli.Source, 'datasets', lambda *a, **kw: [])
    commands = iter(['use data1', 'quit'])
    monkeypatch.setattr('builtins.input', lambda *a: next(commands))
    assert cli.main(['dacp://localhost']) == 0
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
    b.onecmd('cat 1')
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
    assert [cell.strip() for cell in lines[1].split('|')[1:-1]] == ['#', 'Path', 'Format', 'Bytes', 'Records', 'Modified (UTC)']
    assert [cell.strip() for cell in lines[3].split('|')[1:-1]] == ['1', '/orders', 'csv', '0', '0', '2026-10-16 06:22:02']
    assert [cell.strip() for cell in lines[4].split('|')[1:-1]] == ['2', r'/file\nname', 'txt', '-', '-', '-']
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
    assert [cell.strip() for cell in plain.split('|')[1:-1]][2:] == ['-', '-', '-', '-']


@pytest.mark.parametrize('command,headers', [('cat 1', ['id'])])
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
    b.onecmd('cat 2 ' + mode)
    rendered = out.getvalue()
    assert not rendered.startswith('+')
    if mode == 'text':
        assert rendered.startswith(r'hello\x1b[31m world')
    else:
        assert rendered.startswith('00000000  68 65 6c 6c 6f')


def test_auto_preview_uses_advertised_views_for_new_formats(browser):
    b, ds, out = browser
    b.items[0].format = 'excel'
    b.onecmd('cat 1')
    assert '3 rows shown' in out.getvalue()


def test_auto_preview_does_not_infer_records_from_filename(browser):
    b, ds, out = browser
    ds.stream.views = ('bytes',)
    b.items[0].path = '/orders.csv'
    b.onecmd('cat 1')
    assert '68 65 6c 6c 6f' in out.getvalue()
    assert 'rows shown' not in out.getvalue()


def test_zip_folder_supports_bytes_preview(browser):
    b, ds, out = browser
    b.items = [SimpleNamespace(path='/archive', kind='folder', format='zip', views=('bytes',))]
    ds.stream.views = ('bytes',)
    b.onecmd('cat 1 hex')
    assert 'folder has no content' not in out.getvalue()
    assert ds.opened == '/archive'
    assert 'Error:' not in out.getvalue()


def test_select_transports_scan_and_caps_preview(browser):
    b, ds, out = browser
    b.onecmd('SELECT id FROM "/orders" WHERE id > 1 LIMIT 2;')
    assert ds.opened == '/orders'
    assert ds.options['columns'] == ['id'] and ds.options['limit'] == 2
    assert ds.options['predicate']._message.op == 'gt'
    assert '| id |' in out.getvalue() and '2 rows shown (limit 2)' in out.getvalue()
    assert ds.stream.cancelled


def test_select_default_and_large_limit_remain_bounded(browser):
    b, ds, out = browser
    b.onecmd('SeLeCt * FROM "/orders" LIMIT 10000')
    assert ds.options['limit'] == 3
    assert '3 rows shown (limit 3)' in out.getvalue()


def test_empty_select_preserves_headers(browser, monkeypatch):
    b, ds, out = browser
    # Real SDK record iterators always support close, including empty streams.
    def empty():
        yield from ()
    monkeypatch.setattr(ds.stream, 'as_records', empty)
    b.onecmd('select id FROM "/orders" WHERE id > 100 LIMIT 0')
    assert ds.options['limit'] == 0
    assert '| id |' in out.getvalue() and '0 rows shown (limit 0)' in out.getvalue()
    assert 'Error:' not in out.getvalue()


def test_select_errors_keep_shell_alive(browser):
    b, ds, out = browser
    b.onecmd('SELECT * FROM "/orders" ORDER BY id')
    b.onecmd('SELECT missing FROM "/orders"')
    ds.stream.views = ('bytes',)
    b.onecmd('SELECT * FROM "/file name"')
    assert 'unsupported SQL syntax' in out.getvalue()
    assert 'unknown or ambiguous column' in out.getvalue()
    assert 'requires an Item with a records view' in out.getvalue()
    assert not ds.closed


def test_select_number_uses_latest_ls_order(browser, monkeypatch):
    b, ds, out = browser
    items = ds.list()
    # The number follows the latest listing, even after its order changes.
    monkeypatch.setattr(ds, 'list', lambda path=None: list(reversed(items)))
    b.onecmd('ls /reports')
    b.onecmd('SELECT id FROM 2 WHERE id > 1 LIMIT 1')
    assert ds.opened == '/orders'
    assert ds.options['columns'] == ['id'] and ds.options['limit'] == 1
    assert ds.options['predicate']._message.values[0].value == '1'
    assert '1 rows shown (limit 1)' in out.getvalue()


@pytest.mark.parametrize('number', ['0', '3'])
def test_select_number_out_of_range_does_not_open(browser, number):
    b, ds, out = browser
    b.onecmd('SELECT * FROM ' + number)
    assert 'item number out of range; use ls' in out.getvalue()
    assert not hasattr(ds, 'opened') and not ds.closed


def test_run_heredoc_source_preserves_indentation(browser, monkeypatch):
    b, ds, out = browser
    b.use_rawinput = False
    b.stdin = io.StringIO("def process(inputs, params):\n    yield b'hi'\nSTOP\n")
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd('run --runtime python@1.0 --input source=1 <<STOP')
    import json
    task = json.loads(calls[0][1])['tasks'][-1]
    assert task['operator'] == {'runtime':'python@1.0', 'source':"def process(inputs, params):\n    yield b'hi'\n"}
    assert task['inputs']['source'] == 'read_1'
    reader = json.loads(calls[0][1])['tasks'][0]
    assert reader == {'id':'read_1','operator':'read_item@1.0','inputs':{},'params':{'uri':'dacp://localhost/data1/orders','view':'bytes'}}


@pytest.mark.parametrize('command', [
    'run --source code.py --input source=1',
    'run --code hello --runtime python@1.0 --input source=1',
    'run identity@1.1 --runtime python@1.0 --input source=1 <<END',
    'run --runtime python@1.0 --input source=1',
])
def test_run_rejects_invalid_source_forms(browser, monkeypatch, command):
    b, ds, out = browser
    monkeypatch.setattr(b, 'print_job', lambda *args: pytest.fail('must not submit'))
    b.onecmd(command)
    assert 'Error:' in out.getvalue()


def test_run_eof_aborts_without_submission(browser, monkeypatch):
    b, ds, out = browser
    b.use_rawinput = False
    b.stdin = io.StringIO('unfinished\n')
    monkeypatch.setattr(b, 'print_job', lambda *args: pytest.fail('must not submit'))
    b.onecmd('run --runtime python@1.0 --input source=1 <<END')
    assert 'nothing submitted' in out.getvalue()


def test_run_ctrl_c_aborts_entry(browser, monkeypatch):
    b, ds, out = browser
    def interrupted(*args): raise KeyboardInterrupt
    monkeypatch.setattr('builtins.input', interrupted)
    monkeypatch.setattr(b, 'print_job', lambda *args: pytest.fail('must not submit'))
    b.onecmd('run --runtime python@1.0 --input source=1 <<END')
    assert 'nothing submitted' in out.getvalue()


def test_run_repository_id(browser, monkeypatch):
    b, ds, out = browser
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd('run identity@1.1 --input source=1')
    import json
    assert json.loads(calls[0][1])['tasks'][-1]['operator'] == 'identity@1.1'


class JobEvents:
    def __init__(self, events):
        self.events = iter(events)
        self.closed = False
        self.consumed = 0
    def __enter__(self): return self
    def __exit__(self, *args): self.closed = True
    def __iter__(self): return self
    def __next__(self):
        self.consumed += 1
        return next(self.events)


def record_start(port='result'):
    import base64
    schema = pa.schema([('id', pa.int64())])
    return dict(type='start',port=port,representation='records',format='arrow',
                schema=base64.b64encode(schema.serialize().to_pybytes()).decode())


def test_job_records_header_once_and_no_implicit_limit(browser, monkeypatch):
    b,ds,out = browser
    stream = JobEvents([record_start(),dict(type='data',port='result',batch=pa.record_batch([[1,2]],names=['id'])),
                        dict(type='data',port='result',batch=pa.record_batch([[3,4]],names=['id'])),
                        dict(type='end',port='result'),dict(type='complete')])
    monkeypatch.setattr(cli.Job,'run',lambda *a,**kw:stream)
    b.print_job('dacp://localhost','job: test',30)
    assert out.getvalue().count('[result] id')==1
    assert '[result] 4' in out.getvalue() and 'Job complete.' in out.getvalue()
    assert stream.closed


def test_record_preview_stops_and_closes_without_reading_rest(browser, monkeypatch):
    b,ds,out = browser
    stream = JobEvents([record_start(),dict(type='data',port='result',batch=pa.record_batch([[1,2,3]],names=['id']))])
    monkeypatch.setattr(cli.Job,'run',lambda *a,**kw:stream)
    b.print_job('dacp://localhost','job: test',30,preview_rows=2)
    assert '[result] 2' in out.getvalue() and '[result] 3' not in out.getvalue()
    assert 'remaining job cancelled' in out.getvalue() and 'Job complete.' not in out.getvalue()
    assert stream.closed and stream.consumed==2


def test_text_ports_and_utf8_across_chunks(browser, monkeypatch):
    b,ds,out = browser
    start = lambda port:dict(type='start',port=port,representation='bytes',format='text')
    stream=JobEvents([start('a'),start('b'),dict(type='data',port='a',data=b'\xe4'),
                      dict(type='data',port='b',data=b'hello'),dict(type='data',port='a',data=b'\xb8\xad\n'),
                      dict(type='end',port='a'),dict(type='end',port='b'),dict(type='complete')])
    monkeypatch.setattr(cli.Job,'run',lambda *a,**kw:stream)
    b.print_job('dacp://localhost','job: test',30)
    assert '[b] hello\n[a] 中\n' in out.getvalue()


def test_binary_preview_counts_bytes_and_has_offsets(browser, monkeypatch):
    b,ds,out = browser
    stream=JobEvents([dict(type='start',port='x',representation='bytes',format='binary'),
                      dict(type='data',port='x',data=bytes(range(32)))])
    monkeypatch.setattr(cli.Job,'run',lambda *a,**kw:stream)
    b.print_job('dacp://localhost','job: test',30,preview_bytes=18)
    assert '[x] 00000010  10 11' in out.getvalue()
    assert '12 13' not in out.getvalue() and stream.closed


def test_datasets_and_directory_only_startup(monkeypatch):
    from dacp.source import DatasetInfo
    calls = []
    entries = [DatasetInfo('orders', 'dacp://node:8815/orders', '{"summary":"公开目录"}'), DatasetInfo('other', 'dacp://node:8815/other')]
    def datasets(url, **credentials):
        calls.append((url, credentials))
        return entries
    monkeypatch.setattr(cli.Source, 'datasets', datasets)
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: (_ for _ in ()).throw(AssertionError('directory must not open data')))
    output = io.StringIO()
    b = cli.Browser('dacp://node:8815', credentials={'cert_chain':b'cert'}, stdout=output)
    try:
        assert b.dataset is None
        b.onecmd('datasets')
        assert 'orders' in output.getvalue() and '公开目录' in output.getvalue()
        b.onecmd('datasets dacp://node:8815')
        assert 'datasets accepts no arguments' in output.getvalue()
        assert calls[-1][0] == 'dacp://node:8815' and calls[-1][1]['cert_chain'] == b'cert'
        assert 'local_only' not in calls[-1][1]
        b.onecmd('ls')
        assert 'select a Dataset first' in output.getvalue()
        ds = FakeDataset()
        monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: ds)
        b.onecmd('use orders')
        assert b.dataset is ds
    finally:
        b.close()


@pytest.mark.parametrize('uri', ['dacp://localhost/data1', 'dacp://localhost/data1/item'])
def test_startup_rejects_dataset_addresses(uri):
    with pytest.raises(ValueError, match='dacp-server address'):
        cli.Browser(uri)


def test_connect_requires_server_and_resets_selection(browser):
    b, ds, out = browser
    b.onecmd('connect dacp://other/data2')
    assert b.dataset is ds and not ds.closed
    b.onecmd('connect dacp://other:8815')
    assert b.dataset is None and ds.closed
    for command in ('ls', 'count', 'cat', 'select', 'run'):
        b.onecmd(command)
    assert out.getvalue().count('select a Dataset first') == 5


@pytest.mark.parametrize('startup', [True, False])
def test_sole_dataset_is_selected_automatically(monkeypatch, startup):
    from dacp.source import DatasetInfo
    ds=FakeDataset();calls=[];out=io.StringIO()
    monkeypatch.setattr(cli.Source,'datasets',lambda *a,**kw: [] if a[0]=='dacp://empty' else [DatasetInfo('sales data','dacp://untrusted/redirect')])
    def open_dataset(uri,**credentials):
        calls.append((uri,credentials));return ds
    monkeypatch.setattr(cli.Source,'open',open_dataset)
    b=cli.Browser('dacp://node:8815' if startup else 'dacp://empty',credentials={'cert_chain':b'cert'},stdout=out)
    try:
        if not startup:b.onecmd('connect dacp://node:8815')
        assert b.dataset is ds and b.uri=='dacp://node:8815/sales%20data'
        assert calls==[('dacp://node:8815/sales%20data',{'cert_chain':b'cert'})]
        assert b.items and 'Automatically using:' in out.getvalue()
        b.onecmd('ls /');assert 'Error:' not in out.getvalue()
    finally:b.close()
    assert ds.closed


def test_failed_automatic_selection_preserves_existing_dataset(browser,monkeypatch):
    from dacp.source import DatasetInfo
    b,old,out=browser;bad=FakeDataset(broken=True)
    monkeypatch.setattr(cli.Source,'datasets',lambda *a,**kw:[DatasetInfo('new','dacp://other/new')])
    monkeypatch.setattr(cli.Source,'open',lambda *a,**kw:bad)
    b.onecmd('connect dacp://other')
    assert b.dataset is old and b.uri=='dacp://localhost/data1' and not old.closed
    assert bad.closed and 'Error:' in out.getvalue()


def test_submit_policy_and_replay_use_bound_server(browser, monkeypatch, tmp_path):
    import json
    b, ds, out = browser
    job = tmp_path / 'workflow.yaml'
    policy = tmp_path / 'policy.json'
    job.write_text('job: example')
    policy.write_text('{"max_retries":2,"timeout_seconds":120}')
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args, **options: calls.append((args, options)))
    b.onecmd(f'submit {job} --policy {policy}')
    assert calls[0][0][:2] == ('dacp://localhost', 'job: example')
    assert calls[0][1]['execution_policy'] == {'max_retries': 2, 'timeout_seconds': 120}
    b.onecmd('replay execution1')
    assert calls[1][0][:2] == ('dacp://localhost', '')
    assert calls[1][1]['replay_from'] == 'execution1'


def test_union_dataset_names_are_used_directly(browser, monkeypatch):
    from dacp.source import DatasetInfo
    b, ds, out = browser
    entries = [DatasetInfo('ds-2345-6789', 'dacp://localhost/ds-2345-6789'),
               DatasetInfo('ds-abcd-efgh', 'dacp://localhost/ds-abcd-efgh')]
    monkeypatch.setattr(cli.Source, 'datasets', lambda *a, **kw: entries)
    opened = []
    monkeypatch.setattr(cli.Source, 'open', lambda uri, **kw: opened.append(uri) or FakeDataset())
    b.onecmd('datasets')
    assert 'Dataset ID' not in out.getvalue() and 'ds-2345-6789' in out.getvalue()
    b.onecmd('use ds-2345-6789')
    assert opened == ['dacp://localhost/ds-2345-6789']
    assert not hasattr(entries[0], 'id') and not hasattr(entries[0], 'display_name')


def test_partial_union_directory_never_autoselects(monkeypatch):
    from dacp.source import DatasetInfo, DatasetDirectory
    entries = DatasetDirectory([DatasetInfo('ds-2345-6789', 'dacp://localhost/ds-2345-6789')], complete=False, unavailable_sources=1)
    monkeypatch.setattr(cli.Source, 'datasets', lambda *a, **kw: entries)
    monkeypatch.setattr(cli.Source, 'open', lambda *a, **kw: pytest.fail('partial directory must not select'))
    output = io.StringIO()
    with_browser = cli.Browser('dacp://localhost', stdout=output)
    try:
        assert with_browser.dataset is None
        with_browser.onecmd('datasets')
        assert 'Directory incomplete: 1 source(s) unavailable' in output.getvalue()
    finally:
        with_browser.close()


def test_run_zero_input_source(browser, monkeypatch):
    b, ds, out = browser
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd("run live_ticks@1.0 --params '{\"max_records\":3}'")
    import json
    task = json.loads(calls[0][1])['tasks'][0]
    assert task['inputs'] == {}
    assert task['params'] == {'max_records': 3}


def test_dataset_numbers_follow_last_display_and_reset_on_connect(browser, monkeypatch):
    from dacp.source import DatasetInfo
    b, ds, out = browser
    entries = [DatasetInfo('weather', 'dacp://untrusted/weather'),
               DatasetInfo('stations', 'dacp://untrusted/stations')]
    monkeypatch.setattr(cli.Source, 'datasets', lambda *a, **kw: entries)
    opened = []
    monkeypatch.setattr(cli.Source, 'open', lambda uri, **kw: opened.append(uri) or FakeDataset())
    b.onecmd('use 1')
    assert not opened and 'run datasets' in out.getvalue()
    out.truncate(0);out.seek(0)
    b.onecmd('datasets')
    table = out.getvalue()
    assert '| #' in table and '| Name' in table
    assert '| URI' not in table and 'dacp://untrusted' not in table
    # Changing the live catalog must not retarget numbers from the printed list.
    entries.reverse()
    b.onecmd('use 1')
    b.onecmd('use 2')
    assert opened == ['dacp://localhost/weather', 'dacp://localhost/stations']
    b.onecmd('use 0');b.onecmd('use 3')
    assert len(opened) == 2
    b.onecmd('datasets');b.onecmd('use 1')
    assert opened[-1] == 'dacp://localhost/stations'
    b.onecmd('connect dacp://other-server')
    b.onecmd('use 1')
    assert len(opened) == 3 and b.dataset_names == []
    # An empty displayed catalog also clears old row numbers.
    b.onecmd('datasets')
    entries.clear()
    b.onecmd('datasets');b.onecmd('use 1')
    assert b.dataset_names == [] and len(opened) == 3


def test_run_builds_only_stream_references_with_explicit_item_sources(browser, monkeypatch):
    import json
    b, ds, out = browser
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd('run join@1 --input left=1 --input right=2 --view records')
    job = json.loads(calls[0][1])
    assert job['outputs'] == {'result': 'run'}
    assert job['tasks'][-1]['inputs'] == {'left':'read_1','right':'read_2'}
    assert [t['params']['view'] for t in job['tasks'][:-1]] == ['records','records']
    assert all(t['inputs'] == {} and t['operator'] == 'read_item@1.0' for t in job['tasks'][:-1])


@pytest.mark.parametrize('options, expected', [
    ('--output left --output right', {'left': 'run:left', 'right': 'run:right'}),
    ('--no-output', {}),
])
def test_run_explicit_results_and_sink(browser, monkeypatch, options, expected):
    import json
    b, ds, out = browser
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd('run example@1 ' + options)
    assert json.loads(calls[0][1])['outputs'] == expected


def test_run_rejects_conflicting_output_options(browser, monkeypatch):
    b, ds, out = browser
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args: calls.append(args))
    b.onecmd('run example@1 --output result --no-output')
    assert not calls
    assert 'cannot be combined' in out.getvalue()


def test_cat_replaces_show_without_alias(browser):
    b, ds, out = browser
    assert hasattr(b, 'do_cat') and not hasattr(b, 'do_show')
    b.onecmd('help cat')
    assert 'Usage: cat ITEM' in out.getvalue()


def test_submit_loads_variables_json_alongside_policy(browser, monkeypatch, tmp_path):
    b, ds, out = browser
    workflow = tmp_path / 'job.yaml'
    variables = tmp_path / 'vars.json'
    policy = tmp_path / 'policy.json'
    workflow.write_text('job: demo\ntasks: [{params: {limit: $LIMIT}}]')
    variables.write_text('{"LIMIT":123}')
    policy.write_text('{"timeout_seconds":120}')
    calls = []
    monkeypatch.setattr(b, 'print_job', lambda *args, **options: calls.append(options))
    b.onecmd(f'submit {workflow} --vars {variables} --policy {policy}')
    assert calls[0]['variables'] == {'LIMIT': 123}
    assert calls[0]['execution_policy'] == {'timeout_seconds': 120}


@pytest.mark.parametrize('value', ['null', '[]', '123'])
def test_submit_rejects_nonobject_variable_files(browser, monkeypatch, tmp_path, value):
    b, ds, out = browser
    job = tmp_path / 'job.yaml'
    variables = tmp_path / 'vars.json'
    job.write_text('job: demo')
    variables.write_text(value)
    monkeypatch.setattr(b, 'print_job', lambda *a, **k: pytest.fail('must not submit'))
    b.onecmd(f'submit {job} --vars {variables}')
    assert 'variables must be a JSON object' in out.getvalue()

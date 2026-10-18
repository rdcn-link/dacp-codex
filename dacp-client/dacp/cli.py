"""Interactive, bounded previews for a DACP dataset."""
import argparse
import codecs
import re
import cmd
from datetime import timezone
import json
import os
from pathlib import Path
import shlex
from urllib.parse import quote, urlsplit, urlunsplit

from .source import Source
from .jobs import Job
from .errors import user_message
from .sql import parse_query


def safe(value):
    """Keep server-provided control characters out of the terminal."""
    return ''.join(c if c.isprintable() or c == '\n' else repr(c)[1:-1] for c in str(value))


class Browser(cmd.Cmd):
    intro = 'DACP shell. Type help for commands, help select for SQL; quit to exit.'
    prompt = 'dacp-shell> '

    def __init__(self, uri, credentials=None, rows=20, byte_limit=4096, stdout=None):
        super().__init__(stdout=stdout)
        self.credentials = credentials or {}
        self.rows, self.byte_limit = rows, byte_limit
        self.dataset = None
        self.items = []
        self.connect(uri)

    def write(self, value):
        print(safe(value), file=self.stdout)

    def connect(self, uri):
        u = urlsplit(uri)
        if not u.path.strip('/') or '/' in u.path.strip('/'):
            raise ValueError('connect expects dacp://host[:port]/dataset (not an item URI)')
        candidate = Source.open(uri, **self.credentials)
        try:
            items = candidate.list()
        except BaseException:
            candidate.close()
            raise
        old = self.dataset
        self.dataset, self.items = candidate, items
        self.uri = uri
        if old is not None:
            old.close()
        self.write(f'Connected: {uri} ({len(items)} items)')

    def onecmd(self, line):
        try:
            return super().onecmd(line)
        except Exception as error:
            self.write(f'Error: {user_message(error)}')
        except KeyboardInterrupt:
            self.write('Preview interrupted.')

    def emptyline(self):
        pass

    def do_help(self, arg):
        """help [COMMAND]: show commands and usage examples.

        Examples:
          help
          help show
          help run
        """
        super().do_help(arg)
        if not arg.strip():
            self.write("Examples (replace paths with items in your Dataset):")
            self.write("  ls /\n  show /reports/orders records")
            self.write('  SELECT id, amount FROM "/reports/orders" WHERE amount > 10 LIMIT 5')
            self.write("  run identity@1.1 --input source=/reports/orders")
            self.write("  submit examples/convert.yaml")
            self.write("Use help COMMAND for more examples, e.g. help run or help submit.")

    def do_connect(self, arg):
        """connect URI: switch to another server/dataset; preserve connection on failure.

        Example:
          connect dacp://localhost:8815/data1
        """
        self.connect(arg.strip())

    def do_use(self, arg):
        """use DATASET: select a dataset on the current server.

        Example:
          use documents
        Dataset names are supplied by the server administrator; there is no discovery command.
        """
        name = arg.strip()
        if not name or '/' in name or name in ('.', '..'):
            raise ValueError('expected one dataset name')
        u = urlsplit(self.uri)
        self.connect(urlunsplit((u.scheme, u.netloc, '/' + quote(name, safe=''), '', '')))

    def table(self, headers, rows, numeric=(), folders=(), path_column=0):
        """Render sanitized cells; ANSI styling is applied after width calculation."""
        clean = lambda value: safe(value).replace('\n', r'\n')
        headers = tuple(map(clean, headers))
        rows = [tuple(map(clean, row)) for row in rows]
        widths = [max(len(row[col]) for row in [headers, *rows]) for col in range(len(headers))]
        border = '+' + '+'.join('-' * (width + 2) for width in widths) + '+'
        color = (self.stdout.isatty() and 'NO_COLOR' not in os.environ
                 and os.environ.get('TERM') != 'dumb')
        def render(row, highlight=False):
            cells = [value.rjust(width) if col in numeric else value.ljust(width)
                     for col, (value, width) in enumerate(zip(row, widths))]
            if highlight and color:
                cells[path_column] = "\x1b[1;36m" + cells[path_column] + "\x1b[0m"
            print('| ' + ' | '.join(cells) + ' |', file=self.stdout)
        self.write(border)
        render(headers)
        self.write(border)
        for index, row in enumerate(rows):
            render(row, index in folders)
        self.write(border)

    def do_ls(self, arg):
        """ls [PATH]: list all Items recursively, or immediate children of a folder.

        Examples:
          ls
          ls /
          ls /reports
        Item numbers in show, count, SELECT and run refer to the latest listing.
        """
        args = shlex.split(arg)
        if len(args) > 1:
            raise ValueError("usage: ls [absolute-logical-path]")
        self.items = self.dataset.list(args[0]) if args else self.dataset.list()
        headers = ('#', 'Path', 'Format', 'Bytes', 'Records', 'Modified (UTC)')
        rows = []
        for number, item in enumerate(self.items, start=1):
            size = item.total_size_bytes if item.total_size_bytes is not None else '-'
            records = item.record_count if item.record_count is not None else '-'
            modified = (item.last_modified_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        if item.last_modified_time is not None else '-')
            rows.append(tuple(safe(value).replace('\n', r'\n') for value in
                              (number, item.path, item.format or '-', size, records, modified)))
        self.table(headers, rows, numeric=(0, 3, 4), path_column=1,
                   folders={index for index, item in enumerate(self.items) if item.kind == 'folder'})
        self.write(f'{len(self.items)} items')

    def do_count(self, arg):
        """count ITEM: explicitly compute exact records by ls number or quoted path; may be slow.

        Examples:
          count /reports/orders
          count 1
        A number refers to the latest ls result. Unsupported counts display '-'.
        """
        args = shlex.split(arg)
        if len(args) != 1:
            raise ValueError('usage: count ITEM')
        path = self.item(args[0])
        self.write(f'Counting records: {path} ...')
        count = self.dataset.count(path)
        self.write(f'{path}: {count if count is not None else "-"} records')

    def item(self, value):
        if value.isdecimal():
            index = int(value)
            if not 1 <= index <= len(self.items):
                raise ValueError('item number out of range; use ls')
            return self.items[index - 1].path
        return value

    def do_show(self, arg):
        """show ITEM [auto|records|text|hex]: preview an item by ls number or quoted path.

        auto uses the advertised records view when available, otherwise hex.
        records prints at most --rows rows; text/hex reads at most --bytes bytes.

        Examples:
          show /reports/orders records
          show 1
          show "/file name" text
          show /image hex
        A number refers to the latest ls result. Ordinary folders use ls instead.
        """
        args = shlex.split(arg)
        if len(args) not in (1, 2):
            raise ValueError('usage: show ITEM [auto|records|text|hex]')
        path = self.item(args[0])
        listed = next((i for i in self.items if i.path == path), None)
        if listed is not None and listed.kind == "folder" and not listed.views:
            raise ValueError("folder has no content; use ls " + path)
        mode = args[1] if len(args) == 2 else 'auto'
        if mode not in ('auto', 'records', 'text', 'hex'):
            raise ValueError('view must be auto, records, text, or hex')
        with self.dataset.open(path) as stream:
            if mode == 'auto':
                mode = 'records' if 'records' in stream.views else 'hex'
            if mode == 'records':
                self.show_records(stream, self.rows)
            else:
                with stream.as_bytes() as reader:
                    data = reader.read(self.byte_limit)
                if mode == 'text':
                    self.write(data.decode('utf-8', errors='replace'))
                else:
                    for offset in range(0, len(data), 16):
                        chunk = data[offset:offset + 16]
                        text = ''.join(chr(b) if 32 <= b < 127 else '.' for b in chunk)
                        self.write(f'{offset:08x}  {chunk.hex(" "):<47}  {text}')
                self.write(f'{len(data)} bytes shown (limit {self.byte_limit})')

    def show_records(self, stream, limit, headers=None):
        records = stream.as_records()
        remaining = limit
        headers = list(headers or getattr(getattr(stream, "item_schema", None), "names", ()))
        rows = []
        try:
            for batch in records:
                headers = batch.schema.names
                for row in batch.slice(0, remaining).to_pylist():
                    cells, budget = [], self.byte_limit
                    for value in row.values():
                        rendered = (value if isinstance(value, str) else
                                    json.dumps(value, default=str, ensure_ascii=True))
                        rendered = safe(rendered).replace('\n', r'\n')
                        cells.append(rendered[:budget] + (' ...' if len(rendered) > budget else ''))
                        budget = max(0, budget - len(rendered))
                    rows.append(cells)
                    remaining -= 1
                if remaining == 0:
                    break
        finally:
            records.close()
        if not headers:
            schema = getattr(records, 'schema', None)
            headers = schema.names if schema is not None else ['Records']
        self.table(headers, rows)
        self.write(f'{limit - remaining} rows shown (limit {limit})')

    def parseline(self, line):
        command, arg, original = super().parseline(line)
        if command and command.lower() == 'select':
            command = 'select'
        return command, arg, original

    def do_select(self, arg):
        """SELECT columns FROM "/item" or FROM number [WHERE condition] [LIMIT n]: query remote records.

        FROM number uses the latest ls listing.
        Supports *, column lists, comparisons, AND/OR/NOT, IN, and null checks.
        Results are limited to --rows for display, even if SQL LIMIT is larger.

        Examples:
          SELECT * FROM "/reports/orders" LIMIT 5
          SELECT id, amount FROM "/reports/orders" WHERE amount > 10 LIMIT 5
          SELECT * FROM 1 WHERE id IN (1, 2) AND amount IS NOT NULL
        Paths are double-quoted; string literals use single quotes. Only records views support SELECT.
        """
        query = parse_query('SELECT ' + arg, resolve_item=self.item)
        with self.dataset.open(query.path) as metadata:
            if 'records' not in metadata.views:
                raise ValueError('SELECT requires an Item with a records view')
            schema = metadata.item_schema
            predicate = query.predicate(schema)
            if schema is not None and query.columns is not None:
                for name in query.columns:
                    if schema.get_field_index(name) < 0:
                        raise ValueError(f'unknown or ambiguous column: {name}')
        limit = min(self.rows, query.limit) if query.limit is not None else self.rows
        with self.dataset.open(query.path, predicate=predicate, columns=query.columns, limit=limit) as stream:
            self.show_records(stream, limit, headers=query.columns)

    def heredoc(self, delimiter):
        lines = []
        size = 0
        while True:
            if self.use_rawinput:
                line = input('... ')
            else:
                line = self.stdin.readline()
                if line == '':
                    raise ValueError('heredoc ended before delimiter; nothing submitted')
                line = line.removesuffix('\n').removesuffix('\r')
            if line == delimiter:
                break
            size += len(line.encode('utf-8')) + 1
            if size > 512*1024:
                raise ValueError('source exceeds 512 KiB; nothing submitted')
            lines.append(line)
        code = '\n'.join(lines) + '\n'
        if not code.strip():
            raise ValueError('source is empty; nothing submitted')
        return code

    def do_run(self, arg):
        """run OP --input PORT=ITEM [--params JSON]
        run --runtime NAME@VERSION [--entrypoint CLASS] --input PORT=ITEM <<END
        Source follows until END on its own line. Source requires server DEBUG mode.
        Results print incrementally; Ctrl-C cancels entry or execution.
        --preview-rows N or --preview-bytes N cancels the entire job at the first per-port limit.

        Examples:
          run identity@1.1 --input source=/reports/orders
          run identity@1.1 --input source=1 --view records --preview-rows 5
          run format_convert@1.1 --input source=/reports/orders --params '{"from":"csv","to":"parquet"}'
          run native_uppercase@1.0 --input source=/reports/orders --preview-bytes 128

        DEBUG source example (requires a configured python@1.0 runtime):
          run --runtime python@1.0 --input source=/reports/orders <<END
          def process(inputs, params):
              for chunk in inputs['source']:
                  yield chunk.upper()
          END

        Use --input for each named port. ITEM is a path, latest ls number, or full Item URI.
        Optional: --params-file FILE, --timeout SECONDS, --coordinator dacp://host:port.
        Installed operator IDs work in NORMAL mode; native_uppercase requires publishing the native example.
        No preview limit applies by default. Job results are streams, with no saved Dataset.
        """
        tokens = shlex.split(arg)
        delimiter = None
        if tokens:
            last = tokens[-1]
            if last.startswith('<<'):
                delimiter = last[2:]
                tokens.pop()
            elif len(tokens) >= 2 and tokens[-2] == '<<':
                delimiter = tokens.pop()
                tokens.pop()
        if delimiter is not None and not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*', delimiter):
            raise ValueError('heredoc delimiter must be an identifier')
        parser = argparse.ArgumentParser(prog='run', exit_on_error=False)
        parser.add_argument('operator', nargs='?')
        parser.add_argument('--runtime')
        parser.add_argument('--entrypoint')
        parser.add_argument('--input', action='append', required=True)
        parser.add_argument('--view', choices=('bytes','records'), default='bytes')
        parser.add_argument('--params', default='{}')
        parser.add_argument('--params-file')
        parser.add_argument('--coordinator')
        parser.add_argument('--timeout', type=positive, default=3600)
        parser.add_argument('--preview-rows', type=positive)
        parser.add_argument('--preview-bytes', type=positive)
        # argparse's missing/unknown arguments otherwise exit the interactive shell.
        parser.error = lambda message: (_ for _ in ()).throw(ValueError(message))
        options = parser.parse_args(tokens)
        if delimiter is None:
            if not options.operator or options.runtime or options.entrypoint:
                raise ValueError('use an operator ID, or --runtime with a heredoc')
            operator = options.operator
        else:
            if options.operator or not options.runtime:
                raise ValueError('source requires --runtime and a heredoc, without an operator ID')
            operator = {'runtime': options.runtime}
            if options.entrypoint:
                operator['entrypoint'] = options.entrypoint
        params = json.loads(Path(options.params_file).read_text() if options.params_file else options.params)
        if not isinstance(params, dict):
            raise ValueError('params must be a JSON object')
        inputs = {}
        for binding in options.input:
            port, sep, item = binding.partition('=')
            if not sep or not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]{0,127}', port) or port in inputs or not item:
                raise ValueError('each input requires a unique PORT=ITEM')
            if item.startswith('dacp://'):
                uri = item
            else:
                path = self.item(item)
                if not path.startswith('/') or path == '/':
                    raise ValueError('input must name an item path or ls number')
                uri = self.uri.rstrip('/') + quote(path, safe='/')
            inputs[port] = {'uri': uri, 'view': options.view}
        if delimiter is not None:
            try:
                operator['source'] = self.heredoc(delimiter)
            except (KeyboardInterrupt, EOFError):
                self.write('Source entry cancelled; nothing submitted.')
                return
        job = {'job':'shell', 'tasks':[{'id':'run','operator':operator,'inputs':inputs,'params':params}]}
        u = urlsplit(self.uri)
        coordinator = options.coordinator or urlunsplit((u.scheme,u.netloc,'','',''))
        self.print_job(coordinator, json.dumps(job), options.timeout, **({"preview_rows":options.preview_rows,"preview_bytes":options.preview_bytes} if options.preview_rows or options.preview_bytes else {}))

    def do_submit(self, arg):
        """submit [FILE] [--preview-rows N] [--preview-bytes N]: submit job YAML.

        Examples (file paths are relative to the shell working directory):
          submit examples/convert.yaml
          submit examples/convert.yaml --preview-bytes 128

        Without FILE, enter YAML and finish with END alone on a line:
          submit
          job: demo
          tasks:
            - id: copy
              operator: identity@1.1
              inputs:
                source: {uri: 'dacp://localhost:8815/data1/reports/orders', view: bytes}
          END

        Results print incrementally. Preview limits cancel the whole remaining job.
        Ctrl-C cancels entry or execution. Use your node URI and installed operator IDs.
        """
        parser = argparse.ArgumentParser(prog='submit', exit_on_error=False)
        parser.add_argument('file', nargs='?')
        parser.add_argument('--preview-rows', type=positive)
        parser.add_argument('--preview-bytes', type=positive)
        parser.error = lambda message: (_ for _ in ()).throw(ValueError(message))
        options = parser.parse_args(shlex.split(arg))
        try:
            text = Path(options.file).read_text() if options.file else self.heredoc('END')
        except (KeyboardInterrupt, EOFError):
            self.write('Job entry cancelled; nothing submitted.')
            return
        u = urlsplit(self.uri)
        self.print_job(urlunsplit((u.scheme,u.netloc,'','','')), text, 3600,
                       preview_rows=options.preview_rows, preview_bytes=options.preview_bytes)

    def print_job(self, node, text, timeout, *, preview_rows=None, preview_bytes=None):
        decoders, counts, offsets = {}, {}, {}
        text_port, line_start = None, True
        emitted = False
        def text_chunk(port, value):
            nonlocal text_port, line_start
            if not value:
                return
            if text_port != port and not line_start:
                print(file=self.stdout)
                line_start = True
            text_port = port
            for piece in value.splitlines(keepends=True):
                if line_start:
                    print(f'[{safe(port)}] ', end='', file=self.stdout)
                print(safe(piece), end='', file=self.stdout, flush=True)
                line_start = piece.endswith('\n')
        def separate():
            nonlocal line_start
            if not line_start:
                print(file=self.stdout)
                line_start = True
        try:
            with Job.run(node, text, timeout=timeout, **self.credentials) as stream:
                for event in stream:
                    kind, port = event['type'], event.get('port')
                    if kind == 'start':
                        separate()
                        counts[port] = 0
                        offsets[port] = 0
                        self.write(f"[{port}] {event['representation']} ({event.get('format', '')})")
                        if event['representation'] == 'records':
                            # Schema is available before the first batch, including empty results.
                            import base64
                            import pyarrow as pa
                            schema = pa.ipc.read_schema(pa.BufferReader(base64.b64decode(event['schema'])))
                            self.write(f"[{port}] " + '\t'.join(schema.names))
                        elif event.get('format') in ('txt','text','csv','ndjson','json','jsonl'):
                            decoders[port] = codecs.getincrementaldecoder('utf-8')(errors='replace')
                    elif kind == 'data':
                        emitted = True
                        limit = preview_rows if 'batch' in event else preview_bytes
                        remaining = None if limit is None else max(0, limit-counts[port])
                        if 'batch' in event:
                            separate()
                            batch = event['batch']
                            if remaining is not None:
                                batch = batch.slice(0, remaining)
                            for row in batch.to_pylist():
                                cells = [safe(row[name]).replace('\n', r'\n').replace('\t', r'\t') for name in batch.schema.names]
                                self.write(f"[{port}] " + '\t'.join(cells))
                            counts[port] += batch.num_rows
                        else:
                            data = event['data'] if remaining is None else event['data'][:remaining]
                            counts[port] += len(data)
                            if port in decoders:
                                text_chunk(port, decoders[port].decode(data))
                            else:
                                separate()
                                for index in range(0,len(data),16):
                                    chunk = data[index:index+16]
                                    self.write(f"[{port}] {offsets[port]:08x}  {chunk.hex(' ')}")
                                    offsets[port] += len(chunk)
                        if limit is not None and counts[port] >= limit:
                            if port in decoders:
                                text_chunk(port, decoders[port].decode(b'',final=True))
                            separate()
                            self.write(f'Preview limit reached on {port}; remaining job cancelled.')
                            return
                    elif kind == 'end' and port in decoders:
                        text_chunk(port, decoders[port].decode(b'', final=True))
                    elif kind == 'complete':
                        separate()
                        self.write('Job complete.')
                    self.stdout.flush()
        except BaseException:
            if emitted:
                separate()
                self.write('Job interrupted or failed; results above are partial.')
            raise

    def do_quit(self, arg):
        """quit: close the connection and exit. Aliases: exit, EOF (Ctrl-D).

        Examples:
          quit
          exit
        """
        return True

    do_exit = do_quit
    do_EOF = do_quit

    def close(self):
        if self.dataset is not None:
            self.dataset.close()
            self.dataset = None


def positive(value):
    number = int(value)
    if number <= 0:
        raise argparse.ArgumentTypeError('must be greater than zero')
    return number


def main(argv=None):
    parser = argparse.ArgumentParser(prog='dacp-shell', description='Interactive DACP dataset shell (mTLS).')
    parser.add_argument('uri', help='dacp://host[:port]/dataset')
    parser.add_argument('--ca', type=Path, help='CA certificate; defaults to DACP_CA')
    parser.add_argument('--cert', type=Path, help='client certificate; defaults to DACP_CERT')
    parser.add_argument('--key', type=Path, help='client key; defaults to DACP_KEY')
    parser.add_argument('--rows', type=positive, default=20, help='maximum rows per preview (default: 20)')
    parser.add_argument('--bytes', type=positive, default=4096, dest='byte_limit', help='byte preview limit and rendered row character limit (default: 4096)')
    args = parser.parse_args(argv)
    browser = None
    try:
        credentials = {key: path.read_bytes() for key, path in (
            ('tls_root_certs', args.ca), ('cert_chain', args.cert), ('private_key', args.key)
        ) if path is not None}
        browser = Browser(args.uri, credentials, args.rows, args.byte_limit)
        browser.cmdloop()
    except KeyboardInterrupt:
        return 130
    except Exception as error:
        print('Error: ' + safe(user_message(error)))
        return 1
    finally:
        if browser is not None:
            browser.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

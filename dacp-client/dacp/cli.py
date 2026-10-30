"""Interactive, bounded previews for a DACP dataset."""
from . import yaml_document
import argparse
import codecs
import re
import cmd
from datetime import timezone
import json
import os
from pathlib import Path
import shlex
import time
from urllib.parse import quote, urlsplit, urlunsplit

from .source import Source
from .view_preview import block_preview, compact
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
        self.dataset_names = []
        self.connect(uri)

    def write(self, value):
        print(safe(value), file=self.stdout)

    def connect(self, uri):
        u = urlsplit(uri)
        if (u.scheme != 'dacp' or not u.hostname or u.username is not None
                or u.query or u.fragment or u.path not in ('', '/')
                or u.port is not None and not 0 < u.port <= 65535):
            raise ValueError('expected dacp-server address: dacp://host[:port]; select a dataset with use NAME')
        origin = urlunsplit((u.scheme, u.netloc, '', '', ''))
        entries = Source.datasets(origin, **self.credentials)
        candidate, items, selected_uri = None, [], origin
        if len(entries) == 1 and getattr(entries, "complete", True):
            # Reconstruct locally so directory metadata cannot redirect the shell.
            name = entries[0].name
            if not name or '/' in name or name in ('.', '..'):
                raise ValueError('invalid dataset name in directory')
            selected_uri = origin + '/' + quote(name, safe='')
            candidate = Source.open(selected_uri, **self.credentials)
            try:
                items = candidate.list()
            except BaseException:
                candidate.close()
                raise
        old = self.dataset
        self.dataset, self.items, self.uri = candidate, items, selected_uri
        self.dataset_names = []
        if old is not None:
            old.close()
        self.write(f'Connected: {origin} ({len(entries)} datasets)')
        if candidate is not None:
            self.write(f'Automatically using: {selected_uri} ({len(items)} items)')
        else:
            self.write('Select a dataset with use NAME.')

    def onecmd(self, line):
        try:
            command, _, _ = self.parseline(line)
            if self.dataset is None and command in ('ls', 'count', 'cat', 'select', 'run'):
                raise ValueError('select a Dataset first with use NAME; use datasets to list choices')
            return super().onecmd(line)
        except Exception as error:
            self.write(f'Error: {user_message(error)}')
        except KeyboardInterrupt:
            self.write('Preview interrupted.')

    def emptyline(self):
        pass

    def do_help(self, arg):
        """Show available commands or detailed help for one command.

        Usage: help [COMMAND]

        Examples:
          help
          help cat
          help run
        """
        super().do_help(arg)
        if not arg.strip():
            self.write("Examples (replace paths with items in your Dataset):")
            self.write("  datasets\n  use data1\n  ls /\n  cat /reports/orders records")
            self.write('  SELECT id, amount FROM "/reports/orders" WHERE amount > 10 LIMIT 5')
            self.write("  run piflow_csv_parser@1.0 --input source=/reports/orders")
            self.write("  submit examples/convert.yaml")
            self.write("Use help COMMAND for more examples, e.g. help run or help submit.")

    def do_connect(self, arg):
        """Connect to a DACP server and automatically select its sole Dataset.

        Usage: connect URI

        Example:
          connect dacp://localhost:8815
        """
        self.connect(arg.strip())

    def do_datasets(self, arg):
        """List public datasets on the connected server.

        Usage: datasets

        Example:
          datasets
        Shows #, Name and Description (JSON). use NUMBER selects a listed row.
        Uses this shell's server and existing TLS credentials; accepts no arguments.
        Directory access requires no data-access grant.
        """
        if arg.strip():
            raise ValueError('datasets accepts no arguments; it uses the connected server')
        u = urlsplit(self.uri)
        origin = urlunsplit((u.scheme, u.netloc, '', '', ''))
        entries = Source.datasets(origin, **self.credentials)
        self.table(('#', 'Name', 'Description (JSON)'),
                   [(i, m.name, m.description) for i, m in enumerate(entries, 1)], numeric=(0,))
        self.dataset_names = [m.name for m in entries]
        if not getattr(entries, 'complete', True):
            self.write(f"Directory incomplete: {entries.unavailable_sources} source(s) unavailable")

    def do_use(self, arg):
        """Select the Dataset to browse, query, or use as operator input.

        Usage: use NAME_OR_NUMBER

        Example:
          use documents
          datasets
          use 1
        Numbers select rows from the latest datasets output; reconnecting clears them.
        """
        name = arg.strip()
        if re.fullmatch(r'[0-9]+', name):
            number = int(name)
            if not 1 <= number <= len(self.dataset_names):
                raise ValueError('dataset number out of range; run datasets to list choices')
            name = self.dataset_names[number - 1]
        if not name or '/' in name or name in ('.', '..'):
            raise ValueError('expected one dataset name')
        u = urlsplit(self.uri)
        origin = urlunsplit((u.scheme, u.netloc, '', '', ''))
        uri = urlunsplit((u.scheme, u.netloc, '/' + quote(name, safe=''), '', ''))
        candidate = Source.open(uri, **self.credentials)
        try:
            items = candidate.list()
        except BaseException:
            candidate.close()
            raise
        old = self.dataset
        self.dataset, self.items, self.uri = candidate, items, uri
        if old is not None:
            old.close()
        self.write(f'Using: {uri} ({len(items)} items)')

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
        """List all Items recursively, or the immediate children of a folder.

        Usage: ls [PATH]

        Examples:
          ls
          ls /
          ls /reports
        Item numbers in cat, count, SELECT and run refer to the latest listing.
        """
        args = shlex.split(arg)
        if len(args) > 1:
            raise ValueError("usage: ls [absolute-logical-path]")
        self.items = self.dataset.list(args[0]) if args else self.dataset.list()
        headers = ('#', 'Path', 'Format', 'Views', 'Bytes', 'Records', 'Modified (UTC)')
        rows = []
        for number, item in enumerate(self.items, start=1):
            size = item.total_size_bytes if item.total_size_bytes is not None else '-'
            records = item.record_count if item.record_count is not None else '-'
            modified = (item.last_modified_time.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')
                        if item.last_modified_time is not None else '-')
            rows.append(tuple(safe(value).replace('\n', r'\n') for value in
                              (number, item.path, item.format or '-', ', '.join(item.views) or '-', size, records, modified)))
        self.table(headers, rows, numeric=(0, 4, 5), path_column=1,
                   folders={index for index, item in enumerate(self.items) if item.kind == 'folder'})
        self.write(f'{len(self.items)} items')

    def do_count(self, arg):
        """Compute the exact record count of an Item; this may require scanning its contents.

        Usage: count ITEM

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

    def do_cat(self, arg):
        """Preview an Item using an advertised view or a byte display mode.

        Usage: cat ITEM [VIEW|auto|text|hex]

        auto prefers records, then bytes (hex), then the first advertised view.
        records prints at most --rows rows; text/hex reads at most --bytes bytes.
        Scientific views show structure and bounded block samples, not a rendered image.

        Examples:
          cat /reports/orders records
          cat 1
          cat "/file name" text
          cat /image hex
          cat 2 array
          cat /sky.fits/hdu0 image
          cat /genome.fa/sequences sequence
        A number refers to the latest ls result. Ordinary folders use ls instead.
        """
        args = shlex.split(arg)
        if len(args) not in (1, 2):
            raise ValueError('usage: cat ITEM [VIEW|auto|text|hex]')
        path = self.item(args[0])
        listed = next((i for i in self.items if i.path == path), None)
        if listed is not None and listed.kind == "folder" and not listed.views:
            raise ValueError("folder has no content; use ls " + path)
        mode = args[1] if len(args) == 2 else 'auto'
        with self.dataset.open(path) as stream:
            if mode == 'auto':
                mode = ('records' if 'records' in stream.views else
                        'hex' if 'bytes' in stream.views else next(iter(stream.views), None))
            required = 'bytes' if mode in ('text', 'hex') else mode
            if required not in stream.views:
                raise ValueError(f'item does not support {required} view; available: {", ".join(stream.views)}')
            if mode == 'bytes':
                mode = 'hex'
            if mode == 'records':
                self.show_records(stream, self.rows)
            elif mode not in ('text', 'hex'):
                self.show_view(stream, mode)
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

    def show_view(self, stream, view):
        # Descriptor and block metadata remain bounded even for third-party codecs.
        with stream.as_view(view) as reader:
            self.write(f'View: {view}')
            self.write('Structure: ' + compact(reader.descriptor, self.byte_limit))
            rows, elements, used = [], 0, 0
            for chunk in reader:
                preview, count, size = block_preview(view, reader.descriptor, chunk,
                                                      self.rows - elements, self.byte_limit - used)
                if count == 0:
                    break
                rows.append((str(len(rows) + 1), compact(chunk.metadata, self.byte_limit),
                             safe(preview).replace('\n', r'\n')[:self.byte_limit]))
                elements += count
                used += size
                if elements >= self.rows or used >= self.byte_limit:
                    break
            self.table(('Block', 'Location / identity', 'Sample'), rows, numeric=(0,))
            self.write(f'{len(rows)} blocks shown, {elements} sample elements, {used} payload bytes '
                       f'(limits {self.rows} elements / {self.byte_limit} bytes; preview may be partial)')

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
        """Query remote records with filtering, column selection, and an optional row limit.

        Usage: SELECT columns FROM "/item" or FROM number [WHERE condition] [LIMIT n]

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
        """Run an operator on selected inputs and print its output streams as they arrive.

        Usage: run OP [--input PORT=ITEM] [--params JSON]
        run --runtime NAME@VERSION [--entrypoint CLASS] [--input PORT=ITEM] <<END
        Source follows until END on its own line. Source requires server DEBUG mode.
        Results print incrementally; Ctrl-C cancels entry or execution.
        --preview-rows N or --preview-bytes N cancels the entire job at the first per-port limit.

        Examples:
          run live_ticks@1.0 --params '{"max_records":3}'
          run piflow_csv_parser@1.0 --input source=/reports/orders
          run piflow_csv_save@1.0 --input source=1 --view records --preview-rows 5
          run piflow_filter@1.0 --input source=/reports/orders --view records --params '{"condition":"amount >= 20"}'
          run native_uppercase@1.0 --input source=/reports/orders --preview-bytes 128

        DEBUG source example (requires a configured python@1.0 runtime):
          run --runtime python@1.0 --input source=/reports/orders <<END
          def process(inputs, params):
              for chunk in inputs['source']:
                  yield chunk.upper()
          END

        Omit --input for zero-input source operators. Use --input for each named port. ITEM is a path, latest ls number, or full Item URI.
        Output defaults to result. Use --output PORT repeatedly for named outputs, or --no-output for a sink.
        Optional: --params-file FILE, --timeout SECONDS, --policy policy.yaml.
        Installed operator IDs work in NORMAL mode; native_uppercase requires publishing the native example.
        No preview limit applies by default. Job results are streams, with no saved Dataset.
        Execution time is printed after completion, failure or cancellation.
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
        parser.add_argument('--input', action='append', default=[])
        parser.add_argument('--output', action='append', help='output port to receive; repeat for multiple ports')
        parser.add_argument('--no-output', action='store_true', help='run a sink without result streams')
        parser.add_argument('--view', choices=('bytes','records'), default='bytes')
        parser.add_argument('--params', default='{}')
        parser.add_argument('--params-file')
        parser.add_argument('--policy', help='execution policy YAML file')
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
        readers = []
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
            reader = 'read_' + str(len(readers) + 1)
            readers.append({'id': reader, 'operator': 'read_item@1.0', 'inputs': {},
                            'params': {'uri': uri, 'view': options.view}})
            inputs[port] = reader
        if delimiter is not None:
            try:
                operator['source'] = self.heredoc(delimiter)
            except (KeyboardInterrupt, EOFError):
                self.write('Source entry cancelled; nothing submitted.')
                return
        if options.no_output and options.output:
            raise ValueError('--no-output and --output cannot be combined')
        ports = [] if options.no_output else (options.output or ['result'])
        if len(set(ports)) != len(ports) or any(not re.fullmatch(r'[A-Za-z_][A-Za-z0-9_-]{0,127}', p) for p in ports):
            raise ValueError('output ports must be unique identifiers')
        job = {'name':'shell', 'tasks':readers + [{'id':'run','operator':operator,'inputs':inputs,'params':params}],
               'outputs': {p: ('run' if p == 'result' else 'run:' + p) for p in ports}}
        u = urlsplit(self.uri)
        coordinator = urlunsplit((u.scheme,u.netloc,'','',''))
        self.print_job(coordinator, yaml_document.dump(job), options.timeout, **({"preview_rows":options.preview_rows,"preview_bytes":options.preview_bytes} if options.preview_rows or options.preview_bytes else {}),
                       **({"execution_policy":yaml_document.mapping(Path(options.policy).read_text())} if options.policy else {}))

    def do_infer(self, arg):
        """Infer workflow output structures without processing data.

        Usage: infer FILE [--vars FILE]
        Example: infer examples/convert-variables.yaml --vars examples/convert-variables-values.yaml
        Unknown structures remain explicit. This does not preview or execute the workflow.
        """
        parser = argparse.ArgumentParser(prog='infer', exit_on_error=False)
        parser.add_argument('file')
        parser.add_argument('--vars', help='workflow variables YAML mapping file')
        parser.error = lambda message: (_ for _ in ()).throw(ValueError(message))
        options = parser.parse_args(shlex.split(arg))
        variables = yaml_document.mapping(Path(options.vars).read_text()) if options.vars else None
        if options.vars and not isinstance(variables, dict):
            raise ValueError('variables must be a mapping')
        u = urlsplit(self.uri)
        result = Job.infer(urlunsplit((u.scheme, u.netloc, '', '', '')), Path(options.file).read_text(),
                           variables=variables, **self.credentials)
        self.write(json.dumps(result, indent=2, ensure_ascii=False))

    def do_submit(self, arg):
        """Submit a workflow from a YAML file or interactive input and print its result streams.

        Usage: submit [FILE] [--vars FILE] [--preview-rows N] [--preview-bytes N]

        Examples (file paths are relative to the shell working directory):
          submit examples/convert.yaml
          submit examples/convert.yaml --vars variables.yaml
          submit examples/convert.yaml --policy policy.yaml
          submit examples/convert.yaml --preview-bytes 128

        Without FILE, enter YAML and finish with END alone on a line:
          submit
          name: demo
          tasks:
            - id: copy
              operator: read_item@1.0
              inputs: {}
              params:
                uri: dacp://localhost:8815/data1/reports/orders
                view: bytes
          outputs:
            result: copy
          END

        --vars replaces complete $NAME values using a JSON object, preserving types.
        Results print incrementally. Preview limits cancel the whole remaining job.
        Execution time is printed after completion, failure or cancellation.
        Ctrl-C cancels entry or execution. Use your node URI and installed operator IDs.
        """
        parser = argparse.ArgumentParser(prog='submit', exit_on_error=False)
        parser.add_argument('--vars', help='workflow variables YAML mapping file')
        parser.add_argument('--policy', help='execution policy YAML file')
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
        variables = yaml_document.mapping(Path(options.vars).read_text()) if options.vars else None
        if options.vars and not isinstance(variables, dict):
            raise ValueError('variables must be a mapping')
        u = urlsplit(self.uri)
        self.print_job(urlunsplit((u.scheme,u.netloc,'','','')), text, 3600,
                       preview_rows=options.preview_rows, preview_bytes=options.preview_bytes,
                       variables=variables,
                       **({"execution_policy":yaml_document.mapping(Path(options.policy).read_text())} if options.policy else {}))

    def do_replay(self, arg):
        """Replay a job using its committed materialized outputs.

        Usage: replay EXECUTION_ID

        Example: replay 1a2b3c
        Ctrl-C cancels execution. Original job policy and retry budget apply.
        """
        tokens = shlex.split(arg)
        if len(tokens) != 1:
            raise ValueError('usage: replay EXECUTION_ID')
        u = urlsplit(self.uri)
        self.print_job(urlunsplit((u.scheme,u.netloc,'','','')), '', 3600, replay_from=tokens[0])

    def print_job(self, node, text, timeout, *, preview_rows=None, preview_bytes=None, execution_policy=None, replay_from=None, variables=None):
        started = time.perf_counter()
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
        def on_retry(event):
            nonlocal text_port, emitted
            separate()
            self.write(f"Attempt failed; discard all results above for this attempt. Receiving retry {event['retry_count']}.")
            decoders.clear(); counts.clear(); offsets.clear()
            text_port = None
            emitted = False
        options = {}
        if execution_policy is not None:
            options['execution_policy'] = execution_policy
            options['on_retry'] = on_retry
        if replay_from is not None:
            options['replay_from'] = replay_from
            options['on_retry'] = on_retry
        if variables is not None:
            options['variables'] = variables
        seen_execution = None
        try:
            with Job.run(node, text, timeout=timeout, **options, **self.credentials) as stream:
                for event in stream:
                    if event.get('execution_id') and seen_execution is None:
                        seen_execution = event['execution_id']
                        self.write(f'Execution: {seen_execution}')
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
        finally:
            elapsed = time.perf_counter() - started
            separate()
            self.write(f'Execution time: {elapsed:.3f} s')
            self.stdout.flush()

    def do_quit(self, arg):
        """Close the connection and exit the shell.

        Usage: quit (aliases: exit, EOF / Ctrl-D)

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
    parser = argparse.ArgumentParser(prog='dacp-shell', description='Interactive DACP dataset shell (TLS + bearer token).')
    parser.add_argument('uri', help='dacp-server address: dacp://host[:port] (automatically selects a sole Dataset)')
    parser.add_argument('--token-file', type=Path, help='bearer token file; defaults to DACP_TOKEN_FILE')
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
        if args.token_file is not None:
            credentials['token'] = args.token_file.read_text().strip()
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

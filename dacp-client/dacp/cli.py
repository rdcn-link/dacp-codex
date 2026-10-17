"""Interactive, bounded previews for a DACP dataset."""
import argparse
import cmd
import json
from pathlib import Path
import shlex
from urllib.parse import quote, urlsplit, urlunsplit

from .source import Source


def safe(value):
    """Keep server-provided control characters out of the terminal."""
    return ''.join(c if c.isprintable() or c == '\n' else repr(c)[1:-1] for c in str(value))


class Browser(cmd.Cmd):
    intro = 'DACP browser. Type help for commands; quit to exit.'
    prompt = 'dacp> '

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
            self.write(f'Error: {error}')
        except KeyboardInterrupt:
            self.write('Preview interrupted.')

    def emptyline(self):
        pass

    def do_connect(self, arg):
        """connect URI: switch to another server/dataset; preserve connection on failure."""
        self.connect(arg.strip())

    def do_use(self, arg):
        """use DATASET: select a dataset on the current server."""
        name = arg.strip()
        if not name or '/' in name or name in ('.', '..'):
            raise ValueError('expected one dataset name')
        u = urlsplit(self.uri)
        self.connect(urlunsplit((u.scheme, u.netloc, '/' + quote(name, safe=''), '', '')))

    def do_ls(self, arg):
        """ls: refresh and list item numbers, formats, and paths."""
        self.items = self.dataset.list()
        for index, item in enumerate(self.items, 1):
            self.write(f'{index:>4}  {item.format:<10}  {item.path}')
        self.write(f'{len(self.items)} items')

    def item(self, value):
        if value.isdecimal():
            index = int(value)
            if not 1 <= index <= len(self.items):
                raise ValueError('item number out of range; use ls')
            return self.items[index - 1].path
        return value

    def do_show(self, arg):
        """show ITEM [auto|records|text|hex]: preview an item by ls number or quoted path.

        auto uses records for CSV/JSON/Parquet and hex for other formats.
        records prints at most --rows rows; text/hex reads at most --bytes bytes.
        """
        args = shlex.split(arg)
        if len(args) not in (1, 2):
            raise ValueError('usage: show ITEM [auto|records|text|hex]')
        path = self.item(args[0])
        mode = args[1] if len(args) == 2 else 'auto'
        if mode not in ('auto', 'records', 'text', 'hex'):
            raise ValueError('view must be auto, records, text, or hex')
        if mode == 'auto':
            item = next((i for i in self.items if i.path == path), None)
            fmt = item.format.lower() if item else Path(path).suffix.lstrip('.').lower()
            mode = 'records' if fmt in ('csv', 'json', 'ndjson', 'parquet') else 'hex'
        with self.dataset.open(path) as stream:
            if mode == 'records':
                records = stream.as_records()
                remaining = self.rows
                try:
                    for batch in records:
                        self.write(batch.schema)
                        for row in batch.slice(0, remaining).to_pylist():
                            rendered = json.dumps(row, default=str, ensure_ascii=True)
                            self.write(rendered[:self.byte_limit] + (' ...' if len(rendered) > self.byte_limit else ''))
                            remaining -= 1
                        if remaining == 0:
                            break
                finally:
                    records.close()
                self.write(f'{self.rows - remaining} rows shown (limit {self.rows})')
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

    def do_quit(self, arg):
        """quit: close the connection and exit."""
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
    parser = argparse.ArgumentParser(description='Interactive DACP dataset browser (mTLS).')
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
        print('Error: ' + safe(error))
        return 1
    finally:
        if browser is not None:
            browser.close()
    return 0


if __name__ == '__main__':
    raise SystemExit(main())

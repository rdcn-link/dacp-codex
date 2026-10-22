"""Shared validation and bounded materialization for PiFlow record adaptations."""
import pyarrow as pa
import sqlite3

MAX_BYTES = 128 * 1024 * 1024


def params(ctx, required=(), optional=()):
    if set(ctx.params) - set(required) - set(optional) or set(required) - set(ctx.params):
        raise ValueError('missing or unexpected parameters')
    return ctx.params


def columns(text, allow_empty=False):
    if not isinstance(text, str):
        raise ValueError('columns must be a comma-separated string')
    names = [s.strip() for s in text.split(',')]
    if allow_empty and text == '':
        return []
    if not all(names) or len(names) != len(set(names)):
        raise ValueError('column names must be unique and nonempty')
    return names


def indexes(schema, names):
    result = [schema.get_field_index(n) for n in names]
    if any(i < 0 for i in result):
        raise ValueError('unknown or ambiguous column')
    return result


def materialize(source):
    schema = source.schema
    size, batches = 0, []
    for batch in source.records():
        size += batch.nbytes
        if size > MAX_BYTES:
            raise ValueError('materialized input exceeds 128 MiB')
        batches.append(batch)
    return pa.Table.from_batches(batches, schema=schema)


def write_table(ctx, table, port='result'):
    out = ctx.records_output(port, table.schema)
    for batch in table.to_batches(max_chunksize=1024):
        out.write(batch)


def identifier(name):
    return '"' + name.replace('"', '""') + '"'


def sql_value(value):
    import datetime
    import decimal
    if isinstance(value, (datetime.date, datetime.time, datetime.datetime, decimal.Decimal)):
        return str(value)
    if value is not None and not isinstance(value, (str, int, float, bytes)):
        raise ValueError('SQL operators accept scalar columns only')
    return value


def database(table, view):
    connection = sqlite3.connect(':memory:')
    try:
        if len(table.column_names) != len(set(table.column_names)) or not table.column_names:
            raise ValueError('unique, nonempty columns required')
        names = ','.join(identifier(n) for n in table.column_names)
        connection.execute('CREATE TABLE ' + identifier(view) + '(' + names + ')')
        connection.executemany('INSERT INTO ' + identifier(view) + ' VALUES (' + ','.join('?' for _ in table.column_names) + ')',
            (tuple(sql_value(row[n]) for n in table.column_names) for row in table.to_pylist()))
        # User SQL may only read the supplied relation and invoke scalar/aggregate functions.
        allowed = {sqlite3.SQLITE_SELECT, sqlite3.SQLITE_READ, sqlite3.SQLITE_FUNCTION, sqlite3.SQLITE_RECURSIVE}
        connection.set_authorizer(lambda action, *_: sqlite3.SQLITE_OK if action in allowed else sqlite3.SQLITE_DENY)
        ticks = [0]
        def progress():
            ticks[0] += 1
            return int(ticks[0] > 10000)
        connection.set_progress_handler(progress, 1000)
        return connection
    except BaseException:
        connection.close()
        raise


def predicate_indices(batch, condition):
    if not isinstance(condition, str) or not condition.strip():
        raise ValueError('condition must be a nonempty SQLite SQL expression')
    reserved = '__dacp_row_index'
    if reserved in batch.schema.names:
        raise ValueError('reserved SQL column name: ' + reserved)
    table = pa.Table.from_batches([batch]).append_column(reserved, pa.array(range(batch.num_rows)))
    connection = database(table, 'source')
    try:
        return [row[0] for row in connection.execute('SELECT ' + identifier(reserved) + ' FROM source WHERE (' + condition + ')')]
    finally:
        connection.close()


def parse_csv(ctx, header, delimiter, schema):
    import csv
    import io
    import itertools
    if type(header) is not bool or not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError('header must be boolean and delimiter one character')
    if not isinstance(schema, str):
        raise ValueError('schema must be a comma-separated string')
    chunks = iter(ctx.input('source').bytes())
    class Raw(io.RawIOBase):
        def __init__(self):
            self.pending = memoryview(b'')
        def readable(self):
            return True
        def readinto(self, buffer):
            while not self.pending:
                try:
                    self.pending = memoryview(next(chunks))
                except StopIteration:
                    return 0
            count = min(len(buffer), len(self.pending))
            buffer[:count] = self.pending[:count]
            self.pending = self.pending[count:]
            return count
    with io.TextIOWrapper(io.BufferedReader(Raw()), encoding='utf-8', errors='strict', newline='') as stream:
        reader = csv.reader(stream, delimiter=delimiter, strict=True)
        first = next(reader, None)
        if header:
            names = first if first is not None else columns(schema)
            rows = reader
        else:
            names = columns(schema) if schema else ['_' + str(i) for i in range(len(first or []))]
            rows = itertools.chain([] if first is None else [first], reader)
        if not names or any(not n for n in names) or len(names) != len(set(names)):
            raise ValueError('CSV requires unique, nonempty column names')
        target = pa.schema([(n, pa.string()) for n in names])
        output = ctx.records_output('result', target)
        batch = []
        for values in rows:
            if len(values) != len(names):
                raise ValueError('CSV row width differs from schema')
            batch.append(dict(zip(names, values)))
            if len(batch) == 128:
                output.write(pa.RecordBatch.from_pylist(batch, schema=target))
                batch.clear()
        if batch:
            output.write(pa.RecordBatch.from_pylist(batch, schema=target))


def query(ctx, sql):
    from types import SimpleNamespace
    from dacp_piflow_sql import process
    process(SimpleNamespace(params={'sql': sql, 'viewName': 'source'}, input=ctx.input, records_output=ctx.records_output))


def aggregate(ctx, axis, actions):
    if not isinstance(axis, str) or not axis or not isinstance(actions, dict) or not actions:
        raise ValueError('axis and nonempty customizedProperties mapping are required')
    expressions = [identifier(axis)]
    for column, action in actions.items():
        if not isinstance(column, str) or not isinstance(action, str) or action not in {'COUNT', 'SUM', 'AVG', 'MAX', 'MIN'}:
            raise ValueError('aggregate must be COUNT, SUM, AVG, MAX or MIN')
        expressions.append(action + '(' + identifier(column) + ') AS ' + identifier(column + '_' + action))
    query(ctx, 'SELECT ' + ','.join(expressions) + ' FROM source GROUP BY ' + identifier(axis) + ' ORDER BY ' + identifier(axis))

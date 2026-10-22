"""PiFlow ExecuteSQLStop adapted to a bounded, read-only SQLite relation."""
import pyarrow as pa
from dacp_piflow_support import params, materialize, database, write_table, MAX_BYTES


def process(ctx):
    p = params(ctx, ['sql', 'viewName'])
    if not isinstance(p['sql'], str) or not isinstance(p['viewName'], str) or not p['viewName']:
        raise ValueError('sql and viewName must be strings')
    table = materialize(ctx.input('source'))
    connection = database(table, p['viewName'])
    try:
        cursor = connection.execute(p['sql'])
        names = [column[0] for column in cursor.description]
        if len(names) != len(set(names)):
            raise ValueError('query must return unique column names')
        rows, size = [], 0
        for values in cursor:
            row = dict(zip(names, values))
            size += sum(len(v) if isinstance(v, (str, bytes)) else 32 for v in values) + 128
            if size > MAX_BYTES:
                raise ValueError('SQL output exceeds 128 MiB')
            rows.append(row)
        if rows:
            result = pa.Table.from_pylist(rows)
        else:
            # SQLite has no static output type metadata; retain known source types.
            result = pa.Table.from_batches([], schema=pa.schema([
                table.schema.field(n) if n in table.column_names else pa.field(n, pa.null()) for n in names]))
        write_table(ctx, result)
    finally:
        connection.close()


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

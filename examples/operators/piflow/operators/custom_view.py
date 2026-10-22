"""PiFlow CustomView adapted to a CSV output stream instead of a destination path."""
import csv
import io
from dacp_piflow_support import params


def process(ctx):
    p = params(ctx)
    header, delimiter = p.get('header', True), p.get('delimiter', ',')
    if type(header) is not bool or not isinstance(delimiter, str) or len(delimiter) != 1:
        raise ValueError('header must be boolean; delimiter must be one character')
    source = ctx.input('source')
    names = source.schema.names
    if len(names) != len(set(names)):
        raise ValueError('unique column names required')
    output = ctx.bytes_output('result', 'csv')
    buffer = io.StringIO(newline='')
    writer = csv.writer(buffer, delimiter=delimiter, lineterminator='\n')
    def emit(values):
        writer.writerow(values)
        output.write(buffer.getvalue().encode('utf-8'))
        buffer.seek(0)
        buffer.truncate(0)
    if header:
        emit(names)
    for batch in source.records():
        for row in batch.to_pylist():
            if any(isinstance(v, (list, dict)) for v in row.values()):
                raise ValueError('CSV export requires scalar columns')
            emit([row[n] for n in names])


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

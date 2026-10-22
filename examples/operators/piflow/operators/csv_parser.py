"""PiFlow CsvParser adapted to bytes input and stable UTF-8 string columns."""
from dacp_piflow_support import params, parse_csv


def process(ctx):
    p = params(ctx, optional=['header', 'delimiter', 'schema'])
    parse_csv(ctx, p.get('header', True), p.get('delimiter', ','), p.get('schema', ''))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

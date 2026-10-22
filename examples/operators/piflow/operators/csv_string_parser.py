"""PiFlow CsvStringParser: headerless CSV text supplied as a bytes stream."""
from dacp_piflow_support import params, parse_csv


def process(ctx):
    p = params(ctx, optional=['delimiter', 'schema'])
    parse_csv(ctx, False, p.get('delimiter', ','), p.get('schema', ''))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

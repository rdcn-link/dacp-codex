"""PiFlow pie-chart grouped data adapted to Arrow records."""
from dacp_piflow_support import params, aggregate


def process(ctx):
    p = params(ctx, ['dimension', 'indicator', 'indicatorOption'])
    aggregate(ctx, p['dimension'], {p['indicator']: p['indicatorOption']})


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

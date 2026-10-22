"""PiFlow Histogram produces grouped chart data, without rendering an image."""
from dacp_piflow_support import params, aggregate


def process(ctx):
    p = params(ctx, ['abscissa', 'customizedProperties'])
    aggregate(ctx, p['abscissa'], p['customizedProperties'])


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

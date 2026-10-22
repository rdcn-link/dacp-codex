"""PiFlow TableShow returns selected table data; presentation remains the client's job."""
from dacp_piflow_support import params, query


def process(ctx):
    p = params(ctx, optional=['showField'])
    fields = p.get('showField', '*')
    if not isinstance(fields, str) or not fields.strip():
        raise ValueError('showField must be a SQL projection string')
    query(ctx, 'SELECT ' + fields + ' FROM source')


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

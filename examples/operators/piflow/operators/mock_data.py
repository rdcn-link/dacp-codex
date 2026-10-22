"""Generate a bounded random record stream; consume the declared input as a trigger."""
import random
import string
import pyarrow as pa
from dacp_piflow_support import params


def process(ctx):
    p = params(ctx, ['schema', 'count'])
    if not isinstance(p['schema'], str) or not isinstance(p['count'], str):
        raise ValueError('schema and count must be strings')
    count = int(p['count'])
    if not 0 <= count <= 1000000:
        raise ValueError('count must be between 0 and 1000000')
    types = dict(String=pa.string(), Int=pa.int32(), Long=pa.int64(), Float=pa.float32(), Double=pa.float64(), Boolean=pa.bool_())
    fields, kinds = [], []
    for definition in p['schema'].split(','):
        parts = [v.strip() for v in definition.split(':')]
        if len(parts) not in (2, 3) or not parts[0] or parts[1] not in types or (len(parts)==3 and parts[2] not in ('true','false')):
            raise ValueError('expected name:Type[:true|false]')
        fields.append(pa.field(parts[0], types[parts[1]], nullable=len(parts)==3 and parts[2]=='true'))
        kinds.append(parts[1])
    if len(set(f.name for f in fields))!=len(fields):
        raise ValueError('unique field names required')
    source=ctx.input('source')
    for _ in source.records():
        pass
    rng = random.Random()
    generators = dict(String=lambda: ''.join(rng.choices(string.ascii_letters+string.digits,k=10)), Int=lambda:rng.randrange(100),
                      Long=lambda:rng.randrange(-(2**63),2**63), Float=rng.random, Double=rng.random, Boolean=lambda:bool(rng.getrandbits(1)))
    schema=pa.schema(fields)
    out=ctx.records_output('result',schema)
    for start in range(0,count,1024):
        rows=[{f.name:None if f.nullable and rng.getrandbits(1) else generators[k]() for f,k in zip(fields,kinds)} for _ in range(min(1024,count-start))]
        out.write(pa.RecordBatch.from_pylist(rows,schema=schema))


if __name__ == '__main__':
    from dacp_operator import OperatorContext
    with OperatorContext() as context:
        process(context)

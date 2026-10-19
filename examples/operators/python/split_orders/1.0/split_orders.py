"""Two inputs and two outputs; the SDK handles protocol and cleanup."""
import pyarrow.compute as pc
from dacp_operator import OperatorContext

with OperatorContext() as ctx:
    minimum = float(ctx.params.get('min_amount', 20))
    schema = ctx.input('left').schema
    accepted = ctx.records_output('accepted', schema)
    rejected = ctx.records_output('rejected', schema)
    for name in ctx.input_names:
        source = ctx.input(name)
        if source.schema != schema:
            raise ValueError('both inputs must have the same schema')
        for batch in source.records():
            mask = pc.fill_null(pc.greater_equal(batch.column('amount'), minimum), False)
            for output, selected in ((accepted, batch.filter(mask)),
                                     (rejected, batch.filter(pc.invert(mask)))):
                if selected.num_rows:
                    output.write(selected)

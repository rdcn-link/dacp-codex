"""Two inputs and two outputs; the SDK handles protocol and cleanup."""
import pyarrow.compute as pc
from dacp_operator import OperatorContext

def process(ctx):
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


def infer_outputs(inputs, params):
    from dacp_operator import records_schema, descriptor, result
    if set(inputs) != {'left', 'right'}:
        raise ValueError('expected left and right inputs')
    left, right = records_schema(inputs['left']), records_schema(inputs['right'])
    if left is not None and right is not None and left != right:
        raise ValueError('both input schemas must match')
    schema = left if left is not None else right
    return result({p: descriptor(['records'], ['arrow'], schema, 'known' if schema is not None else 'unknown') for p in ('accepted', 'rejected')})


def preview_outputs(inputs, params, limits):
    from dacp_operator import preview_process
    return preview_process(process, inputs, params, limits)


if __name__ == '__main__':
    from dacp_operator import run_operator
    run_operator(process, infer_outputs, preview_outputs=preview_outputs)

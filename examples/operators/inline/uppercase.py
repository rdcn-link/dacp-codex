def process(inputs, params):
    """Named inputs contain byte chunks or Arrow RecordBatches; yield result chunks."""
    for chunk in inputs['source']:
        yield chunk.upper()


def infer_outputs(inputs, params):
    from dacp_operator import descriptor, result
    if set(inputs) != {'source'}:
        raise ValueError('expected source input')
    return result({'result': descriptor(['bytes'], [params.get('output_format', 'binary')], certainty='known')})

def process(inputs, params):
    """Named inputs contain byte chunks or Arrow RecordBatches; yield result chunks."""
    for chunk in inputs['source']:
        yield chunk.upper()

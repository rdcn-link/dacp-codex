"""Stream B conversion through A; nodes configured by scripts/test-collaboration.sh."""
import json
import os
from pathlib import Path
import pyarrow as pa
import pyarrow.flight as flight
import pyarrow.parquet as pq
from dacp import Job, Source
from dacp import dacp_pb2 as pb

A = os.environ.get('DACP_NODE_A', 'dacp://localhost:8815')
B = os.environ.get('DACP_NODE_B', 'dacp://localhost:8816')
text = Path(__file__).with_name('job.yaml').read_text().replace('dacp://localhost:8815', A).replace('dacp://localhost:8816', B)


def read_job(yaml):
    result = bytearray()
    with Job.run(A, yaml, timeout=180) as events:
        for event in events:
            if event['type'] == 'data':
                result.extend(event['data'])
    table = pq.read_table(pa.BufferReader(result))
    assert table.to_pydict() == {'id':[1,2], 'amount':[12.5,30.0]}
    return bytes(result)


original = read_job(text)
assert read_job(text) == original
print('PASS: B converts its item, A consumes B output incrementally; repeated runs preserve Parquet bytes/schema')
# Node A has no source dataset: successful execution requires remote placement/read.
try:
    with Source.open(A+'/csv_data') as ds:
        ds.list()
except Exception as error:
    assert getattr(error,'code',None) == 'NOT_FOUND', error
else:
    raise AssertionError('source unexpectedly exists on coordinator')
bad = json.dumps(dict(job='failed_remote', tasks=[
    dict(id='fail', inputs=dict(source=dict(uri=B+'/csv_data/orders')), operator='missing@1.0'),
    dict(id='blocked', inputs=dict(source=dict(task='fail',port='result')), node=A, operator='identity@1.1')]))
try:
    with Job.run(A,bad,timeout=30) as events:
        list(events)
except flight.FlightError:
    pass
else:
    raise AssertionError('remote failure was ignored')
assert read_job(text) == original
print('PASS: upstream failure cancels the pipeline; both nodes remain usable')
# The exchange endpoint must require a client certificate.
client = flight.FlightClient(A.replace('dacp://','grpc+tls://'), tls_root_certs=Path(os.environ['DACP_CA']).read_bytes())
try:
    try:
        writer,reader = client.do_exchange(flight.FlightDescriptor.for_command(pb.OperatorRequest(job_yaml=text).SerializeToString()),
                                          options=flight.FlightCallOptions(timeout=3))
        writer.begin(pa.schema([('data',pa.binary())]));writer.done_writing()
        list(reader)
    except flight.FlightError:
        pass
    else:
        raise AssertionError('job accepted a connection without a certificate')
finally:
    client.close()
print('PASS: job submission requires mTLS')

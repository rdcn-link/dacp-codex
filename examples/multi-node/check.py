"""Run with both example nodes listening and DACP_CA/CERT/KEY set."""
from pathlib import Path
import uuid
from dacp import Job, Source
import pyarrow as pa
import pyarrow.flight as flight
from dacp import dacp_pb2 as pb

suffix = uuid.uuid4().hex[:10]
yaml = Path(__file__).with_name("job.yaml").read_text()
for name in ("b_converted", "a_relay", "b_joined"):
    yaml = yaml.replace(name, name + "_" + suffix)
outputs = Job.run("dacp://localhost:8815", yaml, timeout=180)
assert outputs["b_converted_" + suffix].startswith("dacp://localhost:8816/")
assert outputs["a_relay_" + suffix].startswith("dacp://localhost:8815/")
assert outputs["b_joined_" + suffix].startswith("dacp://localhost:8816/")

def read(uri):
    with Source.open(uri) as ds:
        items = ds.list()
        data = []
        for item in items:
            assert item.schema is not None and item.format == "parquet"
            stream = ds.open(item.path)
            assert sum(batch.num_rows for batch in stream.as_records()) == 2
            with stream.as_bytes() as raw:
                data.append(raw.read())
        return data

original = read(outputs["b_converted_" + suffix])
assert len(original) == 1
assert read(outputs["a_relay_" + suffix]) == original
assert read(outputs["b_joined_" + suffix]) == original * 2
print("PASS: automatic B placement -> A pulls B -> B materializes A+B; bytes and schemas preserved")
# Node A has no source dataset; this job could only succeed through remote placement.
with Source.open("dacp://localhost:8815/csv_data") as ds:
    try: ds.list()
    except pa.ArrowKeyError: pass
    else: raise AssertionError("source unexpectedly exists on coordinator")

bad = f"""job: failed_remote
tasks:
 - {{id: fail, input: 'dacp://localhost:8816/csv_data', operator: missing@1.0, output: failed_{suffix}}}
 - {{id: blocked, input: failed_{suffix}, node: 'dacp://localhost:8815', operator: identity@1.0, output: blocked_{suffix}}}
"""
try: Job.run("dacp://localhost:8815", bad, timeout=30)
except flight.FlightError as error:
    assert "missing" in str(error), error
else: raise AssertionError("remote failure was ignored")
with Source.open(f"dacp://localhost:8815/blocked_{suffix}") as ds:
    try: ds.list()
    except pa.ArrowKeyError: pass
    else: raise AssertionError("downstream executed after upstream failure")
# A failed job does not stop the shared node's data service.
assert read(outputs["a_relay_" + suffix]) == original
print("PASS: remote failure propagates, downstream is skipped, servers remain usable")
print(outputs)

# Control actions, just like DoGet, must reject a connection without a client certificate.
client = flight.FlightClient("grpc+tls://localhost:8815",
                             tls_root_certs=Path("examples/certs/ca.crt").read_bytes())
try:
    try:
        list(client.do_action(flight.Action("dacp.run_job", pb.RunJobRequest(yaml="job: unauthenticated").SerializeToString()),
                              options=flight.FlightCallOptions(timeout=3)))
    except flight.FlightError: pass
    else: raise AssertionError("control action accepted a client without a certificate")
finally:
    client.close()
print("PASS: mTLS is mandatory for job submission")

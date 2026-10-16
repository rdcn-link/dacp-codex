from pathlib import Path
import pyarrow.flight as flight
from dacp import Source
ca = Path("examples/certs/ca.crt").read_bytes()
client = flight.FlightClient("grpc+tls://localhost:8815", tls_root_certs=ca)
try:
    client.get_flight_info(flight.FlightDescriptor.for_path("datasets", "data1"), options=flight.FlightCallOptions(timeout=3))
except flight.FlightError:
    print("mTLS rejects missing client certificate")
else:
    raise AssertionError("server allowed unauthenticated client")
finally:
    client.close()
with Source.open("dacp://localhost:8815/forbidden") as ds:
    try:
        ds.list()
    except flight.FlightError as e:
        assert "FORBIDDEN" in str(e)
        print("URI allowlist rejects unauthorized dataset")
    else:
        raise AssertionError("allowlist bypass")

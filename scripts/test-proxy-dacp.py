#!/usr/bin/env python3
"""Verify a staged proxy against an existing TLS DACP service; leave no relay running."""
import argparse
import hashlib
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.parse import urlsplit

from dacp import Source, DacpError


def digest(origin, dataset, item, credentials):
    result = hashlib.sha256()
    size = 0
    with Source.open(f"{origin}/{dataset}", **credentials) as ds:
        with ds.open(item) as stream, stream.as_bytes() as reader:
            for chunk in iter(lambda: reader.read(65536), b""):
                result.update(chunk)
                size += len(chunk)
    return size, result.hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--upstream", required=True, help="dacp://localhost:18861")
    parser.add_argument("--dataset", required=True)
    parser.add_argument("--item", required=True, help="Absolute Item path with bytes view")
    parser.add_argument("--token-file", required=True, type=Path)
    parser.add_argument("--ca", required=True, type=Path)
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    launcher = root / "dacp-proxy-server/target/universal/stage/bin/dacp-proxy-server"
    upstream = urlsplit(args.upstream)
    if upstream.scheme != "dacp" or not upstream.hostname:
        parser.error("--upstream must be a DACP origin")
    credentials = {"tls_root_certs": args.ca.read_bytes(), "token": args.token_file.read_text().strip()}
    direct = digest(args.upstream.rstrip("/"), args.dataset, args.item, credentials)
    # Use localhost so the upstream localhost certificate also verifies through the relay.
    with socket.socket() as reserve:
        reserve.bind(("127.0.0.1", 0))
        port = reserve.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="dacp-proxy-test-") as directory:
        config = Path(directory) / "proxy.yaml"
        config.write_text(f"host: 127.0.0.1\nport: {port}\nupstream:\n  host: '{upstream.hostname}'\n  port: {upstream.port or 8815}\n")
        with (Path(directory) / "relay.log").open("w") as logs:
            process = subprocess.Popen([str(launcher), str(config)], stdout=logs, stderr=logs)
            try:
                deadline = time.monotonic() + 20
                while True:
                    if process.poll() is not None:
                        raise RuntimeError("Proxy exited before startup")
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=.2):
                            break
                    except OSError:
                        if time.monotonic() >= deadline:
                            raise TimeoutError("Proxy startup timed out")
                        time.sleep(.05)
                origin = f"dacp://localhost:{port}"
                direct_catalog = Source.datasets(args.upstream, **credentials)
                proxied_catalog = Source.datasets(origin, **credentials)
                assert [(d.name, d.uri) for d in direct_catalog] == [(d.name, d.uri) for d in proxied_catalog]
                assert digest(origin, args.dataset, args.item, credentials) == direct
                try:
                    with Source.open(f"{origin}/{args.dataset}", **{**credentials, "token": "invalid-proxy-test-token"}) as ds:
                        ds.open(args.item)
                except DacpError as error:
                    assert error.code in ("ACCESS_DENIED", "UNAUTHENTICATED", "PERMISSION_DENIED"), error.code
                else:
                    raise AssertionError("Upstream accepted an invalid Token through the proxy")
                print(f"PASS: TLS catalog, byte stream ({direct[0]} bytes), identical digest, invalid Token rejected")
            finally:
                process.terminate()
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()


if __name__ == "__main__":
    main()

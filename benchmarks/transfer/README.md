# File-transfer benchmark

[中文](README_zh.md) · [Measured report](../../docs/transfer-benchmark.md)

This Linux harness compares DACP/TLS with vsftpd FTPS, plus unencrypted FTP as a reference, on IPv4 loopback only. It starts isolated temporary servers; it does not use remote hosts or existing services.

## Setup

Build the server from the repository root (`(cd dacp-server && sbt stage)`). The staged `dacp-server` and `dacp-admin` launchers must exist under `dacp-server/target/universal/stage/bin/`. Install Python dependencies in a separate environment:

```bash
python3 -m venv /tmp/dacp-transfer-venv
/tmp/dacp-transfer-venv/bin/pip install -r benchmarks/transfer/requirements.txt
```

On Ubuntu, extract vsftpd without installing or starting a system service:

```bash
mkdir -p "$HOME/.local/share/dacp-transfer-tools"
cd "$HOME/.local/share/dacp-transfer-tools"
apt-get download vsftpd
dpkg-deb -x vsftpd_*.deb extracted
ldd extracted/usr/sbin/vsftpd
```

All shared libraries must resolve. This fixture requires existing `bin` (home `/bin`) and `nobody` accounts, OpenSSL, Java, and noninteractive `sudo -n` permission for launching vsftpd, owning its configuration/chroot directory and stopping its isolated process groups. It creates no system accounts. Configuration files must be root-owned. Review the harness before granting these permissions.

## Run

Run from the repository root. Output paths must not already exist:

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/suite.py \
  --output "$PWD/.benchmark-runs/transfer-smoke" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile smoke --warmups 0 --repeats 1

/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/suite.py \
  --output "$PWD/.benchmark-runs/transfer-final" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile final --warmups 1 --repeats 3

/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish.py \
  .benchmark-runs/transfer-final/results.json
/tmp/dacp-transfer-venv/bin/python -m unittest discover -s benchmarks/transfer -p 'test_*.py'
```

Smoke uses 1/4 MiB large files and ten 16 KiB small files. Final uses 100 MiB/1 GiB files and 1,000 × 64 KiB files. Both cover seven workloads, two sinks and three methods. The default final batch contains 168 runs, including 42 warmups. Allow several minutes, especially for per-file fsync. Source payloads require about 1.2 GiB plus concurrent received files; retain additional free space.

## Measurement contract

- All files contain deterministic physically written pseudorandom bytes. Each received file is checked for length and SHA-256.
- Four/eight parallel clients each download the same 100 MiB source. Throughput uses aggregate payload bytes.
- Reused connections are prepared outside timing, including first-file metadata. Fresh means one new client per worker per trial, not per small file. Normal per-file metadata remains timed.
- `consume` counts and hashes without writing received payloads. `disk-fsync` additionally times write, flush, fsync and close for every file; directory fsync is excluded. Temporary received files are deleted after timing.
- Setup, source generation and reused-client preparation are excluded. Worker startup, transfer, decode, metadata, checksum and resource sampling are included. Caches remain warm; method order is seeded and interleaved.
- DACP uses a reader Token; FTP methods use read-only anonymous login. TLS methods verify the same certificate/hostname. FTPS protects control and data channels. libcurl enables TLS session caching; vsftpd does not require reuse. No explicit compression is configured.
- Client CPU is process-wide. Server CPU is an approximate process-tree delta including waited children. RSS is sampled every 50 ms and sums process RSS, not PSS or incremental request memory. Existing host activity can affect results.

## Artifacts

The ignored `.benchmark-runs/` directory contains temporary credentials, private TLS keys, configurations, logs, fixtures and detailed results. Do not publish it. Servers stop in `finally`; after an external hard kill, identify only this run's process groups before cleaning up. Never use broad process-name kill commands.

`publish.py` accepts complete final batches and writes paired reports, public measurements and SVG charts under `docs/`. Failures remain visible in raw results; medians use successful measured trials. Review public artifacts before sharing. Source hashes identify the report-generation snapshot; runtime inventory records the tested server and SDK artifacts. Only remove run directories that you created, after confirming their processes have stopped.

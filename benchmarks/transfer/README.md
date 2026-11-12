# File-transfer benchmark

[中文](README_zh.md) · [Public-WAN CSV filtering report](../../docs/csv-filter-benchmark.md)

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

The raw-transfer suite is a diagnostic harness; its full-download report is not part of the published manual. Failures remain visible in raw results; medians use successful measured trials. Review public artifacts before sharing. Source hashes identify the report-generation snapshot; runtime inventory records the tested server and SDK artifacts. Only remove run directories that you created, after confirming their processes have stopped.

## CSV attribute-filter benchmark

This additional workload compares DACP server-side record filtering with streaming client-side PyArrow filtering over FTP and FTPS. It uses isolated loopback servers, public data, verified TLS, deterministic CSVs, five selectivity thresholds and identical per-column result checks. No result file is written. Connection establishment is excluded; reads, parsing, filtering, transfer and validation are timed.

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \
  --output "$PWD/.benchmark-runs/csv-filter-final" \
  --vsftpd "$HOME/.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd" \
  --profile final --warmups 1 --repeats 3
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter.py \
  .benchmark-runs/csv-filter-final/results.json
```

Use `--profile smoke --warmups 0 --repeats 1` for a small validation run. Final fixtures have 65,536 and 655,360 rows (about 9 and 89 MiB); the default final batch contains 120 trials. Setup and cleanup requirements above also apply. The publisher rejects failed, incomplete or unverified result batches. Source files and temporary credentials remain in the ignored run directory; results are not published automatically.

### Run the CSV workload over a public WAN

`filter_suite.py --remote-config` uses existing remote DACP/TLS, FTPS and FTP endpoints instead of starting local servers. It preserves the same two CSV fixtures, five thresholds, seeded order, warmups/repeats and exact per-column checks. FTP/FTPS continue parsing directly from the data socket, without a download-to-disk phase. The process bypasses environment proxies to measure the direct route. DACP explicitly waits for connection readiness before timing; normal per-item metadata remains timed.

Use a new isolated remote directory, such as `/var/snap/docker/common/dacp-benchmarks/csv-filter-20261108-wan`. Stage these files there:

- `node/`: a copy of the exact `dacp-server/target/universal/stage/` distribution.
- `vsftpd`: the extracted executable, with its shared libraries already available.
- `data/`: the original `observations-65536.csv` and `observations-655360.csv` files.
- `fixtures.json`: the `fixtures` object from the previous complete local `filter_suite.py` result. Verify all source and executable/JAR digests against that run.
- `remote_filter_fixture.py`: this directory's deployment script.

Do not copy existing credentials or API configuration. On the remote host, with root privileges:

```bash
python3 remote_filter_fixture.py start \
  --root /var/snap/docker/common/dacp-benchmarks/csv-filter-20261108-wan \
  --public-ip 39.106.98.67 --client-ip 60.245.194.13
```

Replace the allowed client IP as needed. Ports are DACP `28801`, FTPS `28802`, FTP `28803`, FTPS passive `28810–28819` and FTP passive `28820–28829`. The script checks that ports are unused, verifies source hashes, creates a private temporary certificate with the public IP in its SAN, and scopes firewall rules to loopback and the specified client. The cloud firewall must also permit these ports. It disables the DACP dashboard and S3, starts no Union, and creates no system account or system service. Only deterministic synthetic data are published anonymously; FTP is read-only. Existing applications and websites are unchanged.

Copy only `tls/cert.pem` back over trusted SSH and record its local absolute path. The private key stays on the server. Prepare a private `remote-config.json` with these fields:

- `host`: `39.106.98.67`.
- `ports`: `{"dacp-tls":28801,"ftps":28802,"ftp-plain":28803}`.
- `ca`: local absolute path of the verified `cert.pem`.
- `fixtures`: complete original `fixtures.json` content, including expected counts and per-column digests.
- `network`: description of the direct public-WAN route and shaping, if any.
- `environment`: actual server OS, CPU count/model, memory, Java, JVM heap and other load.
- `runtime_inventory`: remotely verified server JAR and FTP-binary SHA-256 values.

Run on the client, then explicitly choose an ignored report directory:

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/filter_suite.py \
  --remote-config .benchmark-runs/csv-filter-wan-staging/remote-config.json \
  --output .benchmark-runs/csv-filter-wan-20261108 \
  --profile final --warmups 1 --repeats 3
/tmp/dacp-transfer-venv/bin/python benchmarks/transfer/publish_filter_wan.py \
  .benchmark-runs/csv-filter-wan-20261108/results.json \
  --output .benchmark-runs/csv-filter-wan-20261108/report
```

This contains 120 trials, including 30 warmups. The separate WAN publisher requires a complete successful batch, validates every result and writes English/Chinese reports, charts and raw measurements only to the requested directory. It does not publish to official documentation or websites. It distinguishes actual selectivity from nominal thresholds, Arrow logical memory from wire bytes, and end-to-end task time from isolated protocol speed. It retains every outcome, including high-selectivity cases unfavorable to DACP.

Client CPU/RSS are sampled as before. Remote server CPU/RSS are not collected: their fields are `null`, not sampled from a coincidentally matching local PID. Differences in hardware, network and shared-server load must be stated. Do not extrapolate a universal speedup from this experiment.

Stop the temporary remote services with the same arguments and `stop` in place of `start`. The script verifies registered process start times, stops only its own process groups and removes only its tagged firewall rules. Data/distributions remain for reruns. Never disable certificate verification or use broad process-name kill commands.

### Publish the two verified WAN reports

From the DACP checkout, explicitly publish the CSV-filter and CONET check-dam batches together. This authoring command bundles reports, SVGs and verified JSON under `docs/`; it has no runtime dependency on CONET. Do not copy source data or credentials.

```bash
/tmp/dacp-transfer-venv/bin/python benchmarks/publish-wan-reports.py \
  --conet-home /home/cnic/conet \
  --check-dams-results /home/cnic/conet/.state/benchmarks/check-dams-wan-verified-20261108/results.json \
  --csv-filter-results .benchmark-runs/csv-filter-wan-20261108/results.json
```

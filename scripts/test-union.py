#!/usr/bin/env python3
"""Authenticated smoke test against a running Union and a public-namespace workflow.

Example:
  .venv/bin/python scripts/test-union.py --url dacp://localhost:19860 \
    --ca examples/certs/ca.crt --token-file /protected/client.token --job job.yaml

The workflow must use Union dataset names and optional logical node names. This
script never changes membership or credentials and cancels incomplete execution.
"""
import argparse
import json
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dacp-client'))
from dacp import Source, Union


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--url', required=True)
    parser.add_argument('--ca', type=Path, required=True)
    parser.add_argument('--token-file', type=Path, required=True)
    parser.add_argument('--job', type=Path, required=True)
    parser.add_argument('--vars', type=Path)
    parser.add_argument('--expected', type=Path, help='Compare concatenated bytes output to this file')
    args = parser.parse_args()
    token = args.token_file.read_text().strip()
    variables = json.loads(args.vars.read_text()) if args.vars else None
    datasets = Source.datasets(args.url, tls_root_certs=args.ca.read_bytes(), token=token)
    print(f'Authenticated catalog: {len(datasets)} datasets', flush=True)
    with Union(args.url, token=token, ca=args.ca, timeout=300) as union:
        with union.submit(args.job.read_text(), variables=variables,
                          execution_policy={'materialization': {'allowed': False}}) as run:
            print('Submitted:', run.job_id, flush=True)
            total = 0
            # Retain data only when an explicit byte-for-byte comparison was requested.
            payload = bytearray() if args.expected else None
            for event in run.results():
                if event['type'] == 'data':
                    total += len(event['data'])
                    if payload is not None:
                        payload.extend(event['data'])
            assert run.complete
            assert run.status()['state'] == 'succeeded'
            if payload is not None:
                assert payload == args.expected.read_bytes(), 'output mismatch'
            print(f'PASS: complete result stream ({total} bytes), no materialization')


if __name__ == '__main__':
    main()

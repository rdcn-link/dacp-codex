#!/usr/bin/env python3
"""Compare identical streaming operators locally and across three Union members."""
import argparse
import hashlib
import json
from pathlib import Path
import statistics
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / 'dacp-client'))
from dacp import Union
from dacp.jobs import Job
from dacp import yaml_document


def main():
    p = argparse.ArgumentParser(description=__doc__)
    for key in ['local', 'union', 'local-uri', 'union-uri', 'node-a', 'node-b', 'node-c',
                'ca', 'local-token-file', 'union-token-file', 'output']:
        p.add_argument('--' + key, required=True)
    p.add_argument('--trials', type=int, default=3)
    p.add_argument('--warmups', type=int, default=1)
    a = p.parse_args()
    if a.trials < 1 or a.warmups < 0:
        p.error('trials must be positive and warmups nonnegative')
    directory = Path(a.output); directory.mkdir(parents=True, exist_ok=True)
    policy = {'materialization': {'allowed': False, 'default': 'stream'}}
    variables = {'CONDITION': "quality = '0' AND CAST(temperature AS REAL) >= 30", 'HEADER': True}
    def workflow(distributed):
        tasks = [
            dict(id='read', title='Read weather observations', operator='read_item@1.0', inputs={},
                 params=dict(uri=a.union_uri if distributed else a.local_uri, view='bytes')),
            dict(id='parse', title='Parse weather CSV', operator='piflow_csv_parser@1.0', inputs=dict(source='read:result')),
            dict(id='filter', title='Select valid high temperatures', operator='piflow_filter@1.0', inputs=dict(source='parse:result'), params=dict(condition='$CONDITION')),
            dict(id='export', title='Stream selected records as CSV', operator='piflow_csv_save@1.0', inputs=dict(source='filter:result'), params=dict(header='$HEADER'))]
        if distributed:
            for task, node in zip(tasks, [a.node_a, a.node_a, a.node_b, a.node_c]):
                task['node'] = node
        return yaml_document.dump(dict(name=('Cross-node' if distributed else 'Local') + ' weather streaming benchmark', tasks=tasks, outputs=dict(result='export:result')))
    scripts = {'local': workflow(False), 'cross_node': workflow(True)}
    for name, value in scripts.items():
        (directory / (name + '.yaml')).write_text(value)
    (directory / 'policy.yaml').write_text(yaml_document.dump(policy))
    (directory / 'variables.yaml').write_text(yaml_document.dump(variables))
    ca = Path(a.ca).read_bytes()
    local_token = Path(a.local_token_file).read_text().strip()
    records = []; expected = None
    with Union(a.union, token=Path(a.union_token_file).read_text().strip(), ca=a.ca) as union:
        def run(kind, warmup, index):
            nonlocal expected
            start = time.monotonic(); first = None; size = 0; lines = 0; digest = hashlib.sha256()
            stream = (Job.run(a.local, scripts[kind], tls_root_certs=ca, token=local_token,
                              execution_policy=policy, variables=variables) if kind == 'local' else
                      union.submit(scripts[kind], execution_policy=policy, variables=variables))
            with stream:
                events = stream if kind == 'local' else stream.results()
                for event in events:
                    if event['type'] == 'data':
                        if first is None: first = time.monotonic() - start
                        data = event['data']; size += len(data); lines += data.count(b'\n'); digest.update(data)
                execution = stream.execution_id if kind == 'local' else stream.job_id
                elapsed = time.monotonic() - start
                if kind != 'local':
                    for _ in range(200):
                        status = stream.status()
                        if status['state'] in ('succeeded', 'failed', 'cancelled', 'expired'): break
                        time.sleep(.05)
                    assert status['state'] == 'succeeded', status
            signature = (digest.hexdigest(), size, lines)
            if expected is None: expected = signature
            assert signature == expected, 'local and cross-node results differ'
            record = dict(mode=kind, warmup=warmup, round=index, execution_id=execution,
                          elapsed_seconds=round(elapsed, 6), first_data_seconds=round(first, 6),
                          output_bytes=size, output_rows=max(0, lines-1), sha256=signature[0])
            records.append(record); print(json.dumps(record), flush=True)
            (directory / 'measurements.json').write_text(json.dumps(records, indent=2))
        for i in range(a.warmups):
            for kind in ['local', 'cross_node']: run(kind, True, i+1)
        for i in range(a.trials):
            for kind in (['local', 'cross_node'] if i % 2 == 0 else ['cross_node', 'local']): run(kind, False, i+1)
    medians = {kind: statistics.median(r['elapsed_seconds'] for r in records if r['mode'] == kind and not r['warmup']) for kind in scripts}
    summary = dict(trials=a.trials, warmups_per_mode=a.warmups, median_seconds=medians,
                   cross_node_over_local=medians['cross_node']/medians['local'],
                   records=records, materialization=False,
                   note='Client end-to-end wall time; warmups excluded; identical output hashes. Node placement and deployment network determine applicability.')
    (directory / 'summary.json').write_text(json.dumps(summary, indent=2)); print(json.dumps({k:v for k,v in summary.items() if k!='records'}), flush=True)


if __name__ == '__main__':
    main()

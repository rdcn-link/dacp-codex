"""Run at repository root, with dacp-client installed; C connects only to X."""
from pathlib import Path
import threading
import sys
from dacp import Union, Source

root = Path(__file__).resolve().parents[2]
token=(root/'.union-demo/demo-user.token').read_text().strip()
ca=(root/'examples/certs/ca.crt').read_bytes()
entries=Source.datasets('dacp://localhost:19860',tls_root_certs=ca,token=token)
observations=next(d.uri+'/observations' for d in entries if 'eastern' in d.description)
baselines=next(d.uri+'/station_baselines' for d in entries if 'climate references' in d.description)
with Union('dacp://localhost:19860', token=token, ca=root/'examples/certs/ca.crt', timeout=120) as union:
    with union.submit((root/'examples/union/job.yaml').read_text(),variables={'OBSERVATIONS_URI':observations,'BASELINES_URI':baselines}) as execution:
        def monitor():
            for event in execution.events():
                print(event, file=sys.stderr, flush=True)
        thread = threading.Thread(target=monitor, daemon=True)
        thread.start()
        for event in execution.results():
            if event['type'] == 'data':
                sys.stdout.buffer.write(event['data'])
                sys.stdout.buffer.flush()
        thread.join(timeout=10)

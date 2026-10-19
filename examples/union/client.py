"""Run at repository root, with dacp-client installed; C connects only to X."""
from pathlib import Path
import threading
import sys
from dacp import Union

root = Path(__file__).resolve().parents[2]
with Union('dacp://localhost:19860', cert=root/'examples/certs/client.crt', key=root/'examples/certs/client.key', ca=root/'examples/certs/ca.crt', timeout=120) as union:
    with union.submit((root/'examples/union/job.yaml').read_text()) as execution:
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

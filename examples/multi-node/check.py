"""Run a configured two-node CSV streaming example; credentials use DACP_* variables."""
from pathlib import Path
import os
import yaml
from dacp import Job
source=Path(__file__).with_name('job.yaml').read_text().replace('dacp://localhost:8815',os.environ.get('DACP_NODE_A','dacp://localhost:8815')).replace('dacp://localhost:8816',os.environ.get('DACP_NODE_B','dacp://localhost:8816'))
with Job.run(os.environ.get('DACP_NODE_A','dacp://localhost:8815'),source) as events:
    for event in events:
        if event['type']=='data':print(event['data'].decode(),end='')

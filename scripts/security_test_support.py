"""Protected credentials for isolated live tests; never print issued secrets."""
import json
from pathlib import Path
import subprocess

ROOT = Path(__file__).resolve().parents[1]


def authorize(config, state, *, subject='live-test'):
    """Configure a private credential directory and return a freshly issued token."""
    directory = Path(state) / 'credentials'
    config['credential_directory'] = str(directory)
    result = subprocess.run([
        str(ROOT / 'dacp-server/target/universal/stage/bin/dacp-admin'),
        'create-account', str(directory), '--subject', subject, '--role', 'runner',
    ], capture_output=True, text=True, check=True)
    return json.loads(result.stdout)['token']

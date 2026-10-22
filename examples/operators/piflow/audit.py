#!/usr/bin/env python3
"""Audit upstream provenance and every published manifest/code/source download."""
import argparse
import hashlib
import json
from pathlib import Path
from urllib.parse import quote
from urllib.request import urlopen

HERE=Path(__file__).resolve().parent


def audit(origin):
    inventory=json.loads((HERE/'inventory.json').read_text())
    specifications=json.loads((HERE/'specs.json').read_text())
    classes={s['class'] for s in specifications.values()}
    assert len(classes)==len(specifications)
    for item in inventory['operators']:
        source=HERE/'upstream'/item['source']
        assert hashlib.sha256(source.read_bytes()).hexdigest()==item['sha256'],item['source']
    with urlopen(origin+'/v1/operators',timeout=30) as response:
        manifests=json.load(response)
    expected={i['operator'] for i in inventory['operators'] if i['status']=='published'}
    assert {m['name'] for m in manifests}==expected
    for manifest in manifests:
        provenance = manifest['provenance']
        assert manifest['version']=='1.0'
        assert provenance.get('source_revision')==inventory['commit'] or provenance['source_repository'].endswith('/tree/'+inventory['commit'])
        with urlopen(origin+'/operators/'+manifest['name']+'/1.0',timeout=30) as response:
            html=response.read().decode()
        assert '{{sha256}}' not in html
        for artifact in [manifest['artifact'],manifest['provenance']['source_archive'],manifest['provenance']['license_file']]:
            with urlopen(origin+'/v1/blobs/sha256/'+artifact['sha256']+'/'+quote(artifact['filename']),timeout=30) as response:
                data=response.read()
                assert artifact['filename'] in response.headers.get('Content-Disposition','')
            assert len(data)==artifact['size'] and hashlib.sha256(data).hexdigest()==artifact['sha256']
    print(f'PASS provenance for {len(inventory["operators"])} upstream operators; {len(manifests)} published manifests/pages/code/source/license downloads')


if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--repository',default='http://localhost:8820')
    audit(p.parse_args().repository.rstrip('/'))

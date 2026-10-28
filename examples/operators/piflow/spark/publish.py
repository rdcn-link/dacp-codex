#!/usr/bin/env python3
"""Compile, container-test, and publish one Scala EcoFlow operator."""
import argparse
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

HERE = Path(__file__).resolve().parent
HOME = HERE.parent
ROOT = HOME.parents[2]


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('name')
    p.add_argument('--spark-home', type=Path, required=True)
    p.add_argument('--publish', default='http://localhost:8820')
    p.add_argument('--token-file', type=Path, default=ROOT / '.operator-catalog/.publish-token')
    p.add_argument('--skip-build', action='store_true')
    args = p.parse_args()
    spec = json.loads((HOME / 'specs.json').read_text())[args.name]
    inventory = json.loads((HOME / 'inventory.json').read_text())
    item = next(x for x in inventory['operators'] if x['upstream_class'].endswith('.' + spec['class']))
    jar = HOME / 'target/spark' / args.name / ('dacp-ecoflow-' + args.name + '-1.0.jar')
    if not args.skip_build:
        load(HERE / 'build.py', 'ecoflow_build').build(args.spark_home, args.name)
    load(HERE / 'test.py', 'ecoflow_test').verify(args.name, args.spark_home, docker=True)
    destination = HOME / 'target/catalog'
    destination.mkdir(parents=True, exist_ok=True)
    packaging = load(HOME / 'publish.py', 'ecoflow_packaging')
    files = {}
    for path in HOME.rglob('*'):
        relative = path.relative_to(HOME)
        if path.is_file() and not any(part in ('target', '__pycache__') for part in relative.parts):
            files[str(relative)] = path.read_bytes()
    archive = destination / ('dacp-ecoflow-' + args.name + '-sources.zip')
    archive.write_bytes(packaging.archive(files))
    ports = destination / (args.name + '-ports.json')
    ports.write_text(json.dumps(dict(inputs=spec['inputs'],parameters=spec['parameters'],category=spec['category'],icon=spec['icon'])))
    instructions = destination / (args.name + '-build.txt')
    instructions.write_text('See README_en.md and spark/README_en.md in the source archive.\n'
        'Build shared runtime first. Compile and verify individually:\n'
        '.venv/bin/python examples/operators/piflow/spark/publish.py ' + args.name + ' --spark-home /path/to/extracted/pyspark-4.0.1\n')
    command = ['sudo', '-n', '-u', 'cnic', '-g', 'docker', str(ROOT / 'dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository'),
        'publish', item['operator'] + '@1.0', '--root', str(destination), '--image', 'dacp/operator-ecoflow-spark:1.0',
        '--executor', 'jvm', '--entrypoint', 'dacp.ecoflow.Op_' + args.name, '--artifact', str(jar), '--ports', str(ports),
        '--description', spec['description'], '--source-archive', str(archive), '--source-repository', inventory['repository'],
        '--source-revision', inventory['commit'], '--license', 'Apache-2.0', '--license-file', str(HOME / 'LICENSE'),
        '--build-instructions', str(instructions)]
    subprocess.run(command, check=True)
    load(ROOT / 'scripts/upload-operator.py', 'ecoflow_upload').upload(destination, item['operator'] + '@1.0', args.publish, args.token_file.read_text().strip())
    packaging.mark_published(item['operator'], 'real Arrow IPC contract in an isolated Docker container')
    print('Published and container-verified: ' + item['operator'], flush=True)


if __name__ == '__main__':
    main()

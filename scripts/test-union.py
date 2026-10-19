#!/usr/bin/env python3
"""Real Docker-backed A -> B -> X -> client smoke test. Build both stages first."""
import hashlib
import json
import os
from urllib.request import urlopen
from pathlib import Path
import socket
import ssl
import subprocess
import tempfile
import time
from dacp import Union

ROOT = Path(__file__).resolve().parents[1]
NODE = ROOT/'dacp-server/target/universal/stage/bin/dacp-server'
SCHEDULER = ROOT/'dacp-union-server/target/universal/stage/bin/dacp-union-server'


def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]


def fingerprint(name):
    return hashlib.sha256(ssl.PEM_cert_to_DER_cert((ROOT/f'examples/certs/{name}.crt').read_text())).hexdigest()


def main():
    if not NODE.exists() or not SCHEDULER.exists():
        raise RuntimeError('Run sbt stage in dacp-server and dacp-union-server first')
    processes = []
    logs = []
    with tempfile.TemporaryDirectory(prefix='dacp-union-smoke-') as temporary:
        state = Path(temporary)
        try:
            ports = set()
            while len(ports) < 4:
                ports.add(free_port())
            af, bf, xp, wp = ports
            for node, flight_port, filename in [('a', af, 'sales'), ('b', bf, 'prices')]:
                config = dict(host='127.0.0.1', public_host='localhost', port=flight_port,
                              datasets={'data': {'provider': 'mixed', 'items': {f'/{filename}': {'file': str(ROOT/f'examples/union/{filename}.csv')}}}},
                              allowed_uri_prefixes=[], tls_cert=str(ROOT/f'examples/certs/node-{node}.crt'),
                              tls_key=str(ROOT/f'examples/certs/node-{node}.key'), tls_ca=str(ROOT/'examples/certs/ca.crt'),
                              local_operator_library=str(ROOT/'examples/union/operators'), max_concurrent_tasks=4,
                              union=dict(state_directory=str(state/node), execution_timeout_seconds=90), execution_store=dict(provider="file",settings=dict(directory=str(state/f"{node}-executions"))))
                path = state/f'{node}.yaml'
                path.write_text(json.dumps(config))
                log = (state/f'{node}.log').open('w+')
                logs.append(log)
                processes.append(subprocess.Popen([str(NODE), str(path)], cwd=ROOT/'dacp-server', stdout=log, stderr=subprocess.STDOUT))
            config = dict(host='127.0.0.1', port=xp, public_url=f'dacp://localhost:{xp}',
                          tls_cert=str(ROOT/'examples/certs/server.crt'), tls_key=str(ROOT/'examples/certs/server.key'), tls_ca=str(ROOT/'examples/certs/ca.crt'),
                          state_directory=str(state/'x'), client_identities=[fingerprint('client')], poll_interval_ms=100, execution_timeout_seconds=90, retention_seconds=10, web=dict(host="127.0.0.1",port=wp))
            path = state/'x.yaml'
            path.write_text(json.dumps(config))
            log = (state/'x.log').open('w+')
            logs.append(log)
            processes.append(subprocess.Popen([str(SCHEDULER), 'serve', str(path)], cwd=ROOT/'dacp-union-server', stdout=log, stderr=subprocess.STDOUT))
            for port in [af, bf, xp, wp]:
                deadline = time.monotonic()+30
                while True:
                    if any(p.poll() is not None for p in processes):
                        raise RuntimeError('A service exited during startup')
                    try:
                        with socket.create_connection(('localhost', port), timeout=1):
                            break
                    except OSError:
                        if time.monotonic()>deadline:
                            raise
                        time.sleep(.1)
            def admin(*args):
                result = subprocess.run([str(SCHEDULER), *args], cwd=ROOT/'dacp-union-server', capture_output=True, text=True, check=True)
                return json.loads(result.stdout)
            union_id = admin('identity', str(path))['union_id']
            for node, flight_port, operator in [('a', af, 'quantity_sum@1.0'), ('b', bf, 'revenue_join@1.0')]:
                admin('invite', str(path), f'dacp://localhost:{flight_port}')
                policy = state/f'{node}-policy.yaml'
                policy.write_text(json.dumps(dict(scheduler_identity=fingerprint('server'), input_prefixes=[f'dacp://localhost:{flight_port}/data'], operators=[operator], max_concurrent_tasks=4, expires_at_ms=int((time.time()+600)*1000))))
                subprocess.run([str(NODE), 'union', 'approve', str(state/node), union_id, str(policy)], cwd=ROOT/'dacp-server', check=True, capture_output=True)
            client = Union(f'dacp://localhost:{xp}', cert=ROOT/'examples/certs/client.crt', key=ROOT/'examples/certs/client.key', ca=ROOT/'examples/certs/ca.crt', timeout=90)
            yaml = (ROOT/'examples/union/job.yaml').read_text().replace('18861', str(af)).replace('18862', str(bf))
            with client.submit(yaml) as execution:
                data = bytearray()
                for event in execution.results():
                    if event['type'] == 'data':
                        data.extend(event['data'])
                assert json.loads(data) == {'apple': '9.00', 'pear': '6.00'}, data
                deadline = time.monotonic()+10
                while execution.status()['state'] != 'succeeded':
                    assert time.monotonic()<deadline
                    time.sleep(.05)
                snapshot = execution.status()
                ports_a = snapshot['nodes'][fingerprint('node-a')]['ports']
                ports_b = snapshot['nodes'][fingerprint('node-b')]['ports']
                assert ports_a['ta/outputs/result']['remote_payload_bytes'] > 0
                assert ports_b['tb/inputs/quantities']['remote_payload_bytes'] == ports_a['ta/outputs/result']['payload_bytes']
                assert ports_b['tb/inputs/prices']['remote_payload_bytes'] == 0
                assert list(execution.events())[-1]['snapshot']['state'] == 'succeeded'
                with urlopen(f'http://127.0.0.1:{wp}/api/history') as response:
                    assert json.load(response)['jobs'][0]['state'] == 'succeeded'
                for node, task in [('a', 'ta'), ('b', 'tb')]:
                    records = [json.loads(p.read_text()) for p in (state/f'{node}-executions').glob('*.json')]
                    assert any(r['kind']=='union_task' and r['task_id']==task and r['state']=='succeeded' for r in records)
                if os.environ.get('DACP_TEST_BROWSER'):
                    from playwright.sync_api import sync_playwright
                    with sync_playwright() as playwright:
                        browser = playwright.chromium.launch(executable_path=playwright.chromium.executable_path, headless=True, args=['--no-sandbox'])
                        page = browser.new_page(viewport={'width': 1440, 'height': 1050})
                        errors = []
                        page.on('pageerror', lambda error: errors.append(str(error)))
                        page.goto(f'http://127.0.0.1:{wp}/')
                        page.locator('#jobs .job-link').first.click()
                        page.locator('#tasks .task').nth(1).wait_for()
                        assert page.locator('#ports tr').count() >= 4
                        assert 'tasks:' in page.locator('#job-script').inner_text()
                        assert page.locator('#dag .dag-node').count() == 5
                        assert page.locator('#dag .dag-lane').count() == 3
                        page.locator('#dag-reset').click()
                        assert page.locator('#dag-zoom-label').inner_text() == '100%'
                        page.locator('#dag-zoom-in').click()
                        assert page.locator('#dag-zoom-label').inner_text() == '120%'
                        page.locator('#dag-fit').click()
                        assert page.evaluate("() => [...document.querySelectorAll('#dag .dag-connection')].every(e => {const g=[...document.querySelectorAll('#dag .dag-group')],a=g.find(n=>n.dataset.id===e.dataset.source).querySelector('rect'),b=g.find(n=>n.dataset.id===e.dataset.target).querySelector('rect');return +b.getAttribute('y')>+a.getAttribute('y')+ +a.getAttribute('height');})")
                        assert '汇总本地商品销量' in page.locator('#dag .dag-title').evaluate_all('(nodes)=>nodes.map(n=>n.firstChild.nodeValue)')
                        assert '融合价格计算商品收入' in page.locator('#dag .dag-title').evaluate_all('(nodes)=>nodes.map(n=>n.firstChild.nodeValue)')
                        assert '跨节点' in page.locator('#dag').text_content()
                        assert '成功' in page.locator('#dag').text_content()
                        assert page.locator('#members tr').count() == 2
                        page.screenshot(path='/tmp/union-dashboard-desktop.png', full_page=True)
                        page.select_option('#job-view', 'history')
                        page.locator('#jobs .job-link').first.wait_for()
                        page.locator('#jobs .job-link').first.click()
                        page.locator('#tasks .task').nth(1).wait_for()
                        page.set_viewport_size({'width': 390, 'height': 844})
                        page.screenshot(path='/tmp/union-dashboard-mobile.png', full_page=True)
                        assert not errors, errors
                        browser.close()
                print('PASS: real Docker operators, direct A -> B, B-local fusion, X final relay, Python SDK and status events')
                print(data.decode().strip())
            client.close()
        except BaseException:
            for log in logs:
                log.flush()
                log.seek(0)
                print(log.read()[-12000:])
            raise
        finally:
            for process in reversed(processes):
                process.terminate()
            for process in processes:
                try:
                    process.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            for log in logs:
                log.close()


if __name__ == '__main__':
    main()

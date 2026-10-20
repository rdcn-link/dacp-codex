#!/usr/bin/env python3
"""Browser contract tests for the bundled G6 viewer; requires Playwright and Chromium.

No DACP services, Docker or external network requests are needed.
"""
import argparse
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import mimetypes
from pathlib import Path
import threading
import time
from urllib.parse import urlsplit

from playwright.sync_api import sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WEB = ROOT / 'dacp-union-server/src/main/resources/web'
CSP = "default-src 'none'; script-src 'self'; style-src 'self'; connect-src 'self'; img-src 'self'; frame-ancestors 'none'; base-uri 'none'; form-action 'none'"


def fixture():
    now = int(time.time() * 1000)
    tasks = []
    nodes = {}
    for region, identity, port in [('east', 'a', 18861), ('west', 'c', 18863)]:
        tasks += [dict(id='sum_' + region, title=region + '：汇总商品销量', node=f'dacp://localhost:{port}', node_identity=identity, operator='quantity_sum@1.0', state='succeeded', inputs=[dict(port='source', uri=f'dacp://localhost:{port}/data/sales', view='bytes')]),
                  dict(id='revenue_' + region, title=region + '：融合价格计算收入', node='dacp://localhost:18862', node_identity='b', operator='revenue_join@1.0', state='succeeded', inputs=[dict(port='prices', uri='dacp://localhost:18862/data/prices', view='bytes'), dict(port='quantities', task='sum_' + region, output_port='result')])]
    for t in tasks:
        node = nodes.setdefault(t['node_identity'], dict(observed_at_ms=now, ports={}))
        for direction, ports in [('inputs', [b['port'] for b in t['inputs']]), ('outputs', ['result'])]:
            for port in ports:
                node['ports'][t['id'] + '/' + direction + '/' + port] = dict(payload_bytes=128, remote_payload_bytes=128 if port == 'quantities' else 0, last_activity_ms=now)
    return dict(job_id='g6-browser-test', state='succeeded', owner='client', elapsed_ms=2400, created_at_ms=now-2400, finished_at_ms=now, tasks=tasks, nodes=nodes, events=[], job_yaml='job: g6-browser-test\ntasks: []', outputs={region + '_revenue': dict(task='revenue_' + region, port='result') for region in ['east', 'west']})


class Site(BaseHTTPRequestHandler):
    job = fixture()
    def do_GET(self):
        path = urlsplit(self.path).path
        if path == '/api/overview':
            data = dict(union_id='browser-test', flight_uri='dacp://localhost:19860', at_ms=int(time.time()*1000), poll_interval_ms=500, retention_seconds=86400, job_count=1, active_jobs=0, succeeded_jobs=1, members=[dict(url='dacp://localhost:18861',identity='a'*64,state='approved',observed_at_ms=int(time.time()*1000))], jobs=[self.job])
        elif path == '/api/jobs/g6-browser-test':
            data = self.job
        else:
            local = ROOT / 'assets/dacp-logo.png' if path == '/assets/dacp-logo.png' else WEB / ('index.html' if path == '/' else path.lstrip('/'))
            if (not local.resolve().is_relative_to(WEB.resolve()) and path != '/assets/dacp-logo.png') or not local.is_file():
                self.send_error(404)
                return
            body = local.read_bytes()
            self.send_response(200)
            self.send_header('Content-Type', mimetypes.guess_type(local)[0] or 'application/octet-stream')
            self.send_header('Content-Security-Policy', CSP)
            self.end_headers()
            self.wfile.write(body)
            return
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())
    def log_message(self, *args):
        pass


def main():
    args = argparse.ArgumentParser(description=__doc__)
    args.add_argument('--browser-executable', help='Optional Chromium executable path')
    args.add_argument('--screenshots', type=Path, help='Optional screenshot directory')
    options = args.parse_args()
    server = ThreadingHTTPServer(('127.0.0.1', 0), Site)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    origin = f'http://127.0.0.1:{server.server_port}'
    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, **({'executable_path': options.browser_executable} if options.browser_executable else {}))
            page = browser.new_page(viewport=dict(width=1440, height=1080))
            errors, violations, requests = [], [], []
            page.on('pageerror', lambda e: errors.append(str(e)))
            page.on('console', lambda m: violations.append(m.text) if m.type == 'error' else None)
            page.on('request', lambda r: requests.append(r.url))
            page.goto(origin)
            directory = page.locator('#members .job-link')
            assert directory.inner_text() == '查看数据集'
            assert directory.get_attribute('href') == '/members/' + 'a'*64 + '/datasets'
            styles = page.evaluate("""() => ['#members .job-link','#jobs .job-link'].map(selector=>{
              const s=getComputedStyle(document.querySelector(selector));
              return [s.color,s.fontFamily,s.fontSize,s.textDecorationLine];
            })""")
            assert styles[0] == styles[1]
            page.locator('#jobs .job-link').first.click()
            page.wait_for_function('() => dagViewer.ready')
            page.evaluate('dagViewer.pending')
            assert page.locator('#dag canvas').count() >= 1
            assert page.locator('#dag-error').is_hidden()
            assert page.evaluate('dagViewer.graph.getComboData().length') == 4
            # All dependencies flow left to right, within and across server groups.
            assert page.evaluate('''() => {
              const g=dagViewer.graph;
              return g.getEdgeData().every(e=>g.getElementPosition(e.source)[0]<g.getElementPosition(e.target)[0]);
            }''')
            assert page.evaluate('''() => {
              const groups=dagViewer.graph.getComboData().filter(c=>c.id.startsWith('server:'));
              return new Set(groups.map(c=>c.style.fill)).size===groups.length;
            }''')
            # The server groups must not overlap, even with two parallel branches on B.
            assert page.evaluate('''() => {
              const boxes=dagViewer.graph.getComboData().map(c=>dagViewer.graph.getElementRenderBounds(c.id));
              return boxes.every((a,i)=>boxes.slice(i+1).every(b=>a.max[0]<=b.min[0]||b.max[0]<=a.min[0]||a.max[1]<=b.min[1]||b.max[1]<=a.min[1]));
            }''')
            page.locator('#dag').scroll_into_view_if_needed()
            point = page.evaluate("dagViewer.graph.getViewportByCanvas(dagViewer.graph.getElementPosition('task:sum_east'))")
            box = page.locator('#dag').bounding_box()
            page.mouse.click(box['x'] + point[0], box['y'] + point[1])
            assert 'sum_east' in page.locator('#dag-inspect').inner_text()
            zoom = page.evaluate('dagViewer.graph.getZoom()')
            page.locator('#dag-zoom-in').click()
            page.evaluate('dagViewer.pending')
            assert page.evaluate('dagViewer.graph.getZoom()') > zoom
            # Updating counters and state must preserve layout, pan, zoom and graph instance.
            before = page.evaluate('''() => {window.originalGraph=dagViewer.graph;return {zoom:dagViewer.graph.getZoom(),position:dagViewer.graph.getPosition(),nodes:dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])};}''')
            updated = deepcopy(Site.job)
            updated['state'] = 'running'
            updated['tasks'][0]['state'] = 'running'
            updated['tasks'][1]['state'] = 'waiting_inputs'
            updated['tasks'][2]['state'] = 'unknown'
            updated['tasks'][3]['state'] = 'failed'
            updated['nodes']['a']['ports']['sum_east/outputs/result']['payload_bytes'] = 4096
            updated['nodes']['a']['observed_at_ms'] += 1000
            Site.job = updated
            page.evaluate('refresh()')
            page.evaluate('dagViewer.pending')
            after = page.evaluate('''() => ({zoom:dagViewer.graph.getZoom(),position:dagViewer.graph.getPosition(),nodes:dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])})''')
            assert before == after
            assert page.evaluate('originalGraph === dagViewer.graph')
            assert page.evaluate("dagViewer.graph.getNodeData('task:sum_east').data.outgoing") == '4 KiB'
            assert page.evaluate("dagViewer.graph.getNodeData('task:revenue_east').data.dash.length") > 0
            assert page.evaluate("dagViewer.graph.getNodeData('task:sum_west').data.stateLabel") == '? 状态未知'
            assert '运行中' in page.locator('#dag-inspect').inner_text()
            # Double-click a group's empty header, rather than a task, to collapse it.
            page.locator('#dag-fit').click()
            page.evaluate('dagViewer.pending')
            header = page.evaluate("""() => {const b=dagViewer.graph.getElementRenderBounds('server:0');return dagViewer.graph.getViewportByCanvas([b.min[0]+30,b.min[1]+30]);}""")
            box = page.locator('#dag').bounding_box()
            page.mouse.dblclick(box['x']+header[0], box['y']+header[1])
            page.wait_for_function("() => dagViewer.graph.getComboData('server:0').style.collapsed === true")
            page.evaluate('refresh()')
            assert page.evaluate("dagViewer.graph.getComboData('server:0').style.collapsed")
            page.locator('#dag-expand').click()
            page.evaluate('dagViewer.pending')
            assert not page.evaluate("dagViewer.graph.getComboData('server:0').style.collapsed")
            # Untrusted metadata must remain text in both canvas and DOM inspector.
            updated = deepcopy(Site.job)
            updated['tasks'][0]['title'] = '<img src=x onerror="window.injected=true">'
            Site.job = updated
            page.evaluate('refresh()')
            page.evaluate("dagViewer.inspect('task:sum_east')")
            assert page.locator('#dag-inspect img').count() == 0
            assert page.evaluate('window.injected === undefined')
            assert '<img' in page.locator('#dag-inspect').inner_text()
            if options.screenshots:
                options.screenshots.mkdir(parents=True, exist_ok=True)
                page.locator('#dag').screenshot(path=str(options.screenshots/'dag-desktop.png'))
            page.set_viewport_size(dict(width=390, height=844))
            page.wait_for_timeout(150)
            page.evaluate('dagViewer.pending')
            page.locator('#dag-fit').click()
            page.evaluate('dagViewer.pending')
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            if options.screenshots:
                page.locator('.dag-workspace').screenshot(path=str(options.screenshots/'dag-mobile.png'))
            assert not errors, errors
            assert not violations, violations
            assert all(url.startswith(origin) for url in requests), requests
            browser.close()
            print('G6 browser contracts passed: grouping, clicks, zoom, stable updates, collapse, safe text, mobile and offline assets')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    main()

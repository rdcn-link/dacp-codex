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
    tasks.append(dict(id="prepare", title="准备目标表", node="dacp://localhost:18862", node_identity="b", operator="prepare@1.0", state="succeeded", inputs=[]))
    for t in tasks:
        node = nodes.setdefault(t['node_identity'], dict(observed_at_ms=now, ports={}))
        for direction, ports in [('inputs', [b['port'] for b in t['inputs']]), ('outputs', ['result'])]:
            for port in ports:
                node['ports'][t['id'] + '/' + direction + '/' + port] = dict(payload_bytes=128, remote_payload_bytes=128 if port == 'quantities' else 0, last_activity_ms=now)
    return dict(job_id='g6-browser-test', state='succeeded', owner='client', elapsed_ms=2400, created_at_ms=now-2400, finished_at_ms=now, tasks=tasks, nodes=nodes, events=[], job_yaml='name: g6-browser-test\ntasks: []', attempts=[dict(attempt_id='first',retry_index=0,state='failed',started_at_ms=now-3000,finished_at_ms=now-2500,message='<script>unsafe</script>'),dict(attempt_id='second',retry_index=1,state='succeeded',started_at_ms=now-2400,finished_at_ms=now,nodes=nodes)], outputs={region + '_revenue': dict(task='revenue_' + region, port='result') for region in ['east', 'west']})


class Site(BaseHTTPRequestHandler):
    job = fixture()
    def do_GET(self):
        path = urlsplit(self.path).path
        if path in ('/api/overview', '/api/topology'):
            data = dict(union_id='browser-test', flight_uri='dacp://localhost:19860', at_ms=int(time.time()*1000), poll_interval_ms=500, retention_seconds=86400, job_count=1, active_jobs=0, succeeded_jobs=1, members=[dict(url='dacp://localhost:'+str(18861+i),identity=c*64,state='approved',observed_at_ms=int(time.time()*1000),status=dict(web_enabled=enabled,web_url=url,executors=['jvm'])) for i,c,enabled,url in [(0,'a',True,'http://node-a:19861/'),(1,'b',False,''),(2,'c',True,'javascript:alert(1)')]], jobs=[self.job])
            if path == '/api/topology':
                data.update(stale_after_ms=5000,job_count=1,participation={'a'*64:7,'b'*64:3,'c'*64:2},union_traffic=dict(started_at_ms=1,observed_at_ms=int(time.time()*1000),counters=[dict(origin='dacp://localhost:18861',kind='control',direction='sent',bytes=128),dict(origin='dacp://localhost:18861',kind='stream',direction='received',bytes=1024)]),edges=[dict(source='a'*64,target='b'*64,payload_bytes=2048,observed=True,samples=[dict(key='j/try1/receive/source',bytes=2048,observed_at_ms=int(time.time()*1000),active=True)])])
        elif path == '/api/jobs/g6-browser-test':
            data = self.job
        else:
            assets = {'/assets/' + name: ROOT / 'assets' / name for name in ['dacp-logo.png', 'dacp-favicon.png']}
            assets['/favicon.ico'] = ROOT / 'assets/dacp-favicon.ico'
            assets['/datasets'] = WEB / 'datasets.html'
            assets['/nodes'] = WEB / 'nodes.html'
            local = assets.get(path, WEB / ('index.html' if path == '/' else path.lstrip('/')))
            if (not local.resolve().is_relative_to(WEB.resolve()) and path not in assets) or not local.is_file():
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
    # Keep authored UI text English; fixture titles deliberately retain user Chinese.
    for assets in [WEB, ROOT / 'dacp-server/src/main/resources/node-web']:
        for asset in assets.iterdir():
            if asset.suffix in {'.html', '.js', '.css'}:
                assert not any('\u4e00' <= c <= '\u9fff' for c in asset.read_text()), asset
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
            assert page.locator('html').get_attribute('lang') == 'en-US'
            assert page.locator('h1').inner_text() == 'Collaboration overview'
            assert page.locator('#members .member-datasets-link').count() == 0
            assert page.locator('nav a[href="/datasets"]').count() == 1
            page.locator('#jobs .job-link').first.click()
            page.wait_for_function('() => dagViewer.ready')
            page.evaluate('dagViewer.pending')
            assert page.locator('#job-attempts details').count() == 2
            page.locator('#job-attempts details').first.locator('summary').click()
            assert page.locator('#job-attempts details').first.get_attribute('open') is not None
            assert 'attempts' not in page.locator('#job-policy').inner_text()
            assert page.locator('#job-attempts script').count() == 0
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
            page.evaluate('dagViewer.pending')
            assert page.evaluate("dagViewer.graph.getElementState('task:sum_east')") == ['selected']
            assert page.evaluate("dagViewer.graph.getElementRenderStyle('task:sum_east').lineWidth") == 3.5
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
            assert page.locator('#job-attempts details').first.get_attribute('open') is not None
            assert before == after
            assert page.evaluate('originalGraph === dagViewer.graph')
            assert page.evaluate("dagViewer.graph.getNodeData('task:sum_east').data.outgoing") == '4 KiB'
            assert page.evaluate("dagViewer.graph.getNodeData('task:revenue_east').data.dash.length") > 0
            assert page.evaluate("dagViewer.graph.getNodeData('task:sum_west').data.stateLabel") == '? Unknown status'
            assert 'Running' in page.locator('#dag-inspect').inner_text()
            assert page.evaluate("dagViewer.graph.getElementState('task:sum_east')") == ['selected']
            assert page.evaluate("dagViewer.graph.getElementRenderStyle('task:sum_east').lineWidth") == 3.5
            # Edge selection replaces node selection and survives counter refreshes.
            edge_id = page.evaluate("dagViewer.graph.getEdgeData().find(e=>e.id.startsWith('flow:')).id")
            page.evaluate("id=>dagViewer.graph.emit('edge:click',{target:{id}})", edge_id)
            page.evaluate('dagViewer.pending')
            assert page.evaluate("dagViewer.graph.getElementState('task:sum_east')") == []
            assert page.evaluate('id=>dagViewer.graph.getElementRenderStyle(id).lineWidth', edge_id) == 4
            page.evaluate('refresh()')
            page.evaluate('dagViewer.pending')
            assert page.evaluate('id=>dagViewer.graph.getElementState(id)', edge_id) == ['selected']
            assert page.evaluate('id=>dagViewer.graph.getElementRenderStyle(id).lineWidth', edge_id) == 4
            page.locator('[data-dag-layout="swimlane"]').click()
            page.evaluate('dagViewer.pending')
            assert page.evaluate('id=>dagViewer.graph.getElementState(id)', edge_id) == ['selected']
            page.locator('[data-dag-layout="dependency"]').click()
            page.evaluate('dagViewer.pending')
            assert page.evaluate('id=>dagViewer.graph.getElementRenderStyle(id).lineWidth', edge_id) == 4
            page.evaluate("()=>dagViewer.graph.emit('canvas:click',{})")
            page.evaluate('dagViewer.pending')
            assert page.evaluate('id=>dagViewer.graph.getElementState(id)', edge_id) == []
            assert page.evaluate('id=>dagViewer.graph.getElementRenderStyle(id).lineWidth', edge_id) < 4
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
            dataset_reads = []
            fail_node = [True]
            def datasets_response(route):
                member = route.request.url.split('/members/')[1].split('/')[0]
                dataset_reads.append(member)
                if member == 'c'*64 and fail_node[0]:
                    route.fulfill(status=503, content_type='application/json', body='{}')
                else:
                    entries = [] if member == 'b'*64 else [dict(name='weather',description='{"summary":"<img src=x onerror=alert(1)>"}',uri='dacp://node/weather')]
                    route.fulfill(content_type='application/json',body=json.dumps(dict(datasets=entries,observed_at_ms=1)))
            page.route('**/members/*/datasets', datasets_response)
            page.goto(origin+'/nodes')
            assert page.locator('h1').inner_text() == 'Member nodes'
            page.wait_for_selector('.directory-heading')
            assert page.locator('.member-web-link').count() == 1
            assert page.locator('.member-web-link').get_attribute('href') == 'http://node-a:19861/'
            assert page.locator('.network-node').count() == 3
            assert page.locator('.traffic-wire').count() == 1
            assert 'Union jobs participated: 7 times' in page.locator('.participation-count').first.inner_text()
            assert 'Flight消息' not in page.locator('#network-lines').text_content()
            assert page.locator('.flow-label').count() == 1
            assert page.locator('#network-flows').count() == 0
            assert page.locator('.resource-gauge').count() == 12
            assert '2 KiB' in page.locator('#network-lines').text_content()
            page.evaluate("""async () => {
              const data=await (await fetch('/api/topology')).json();window.rateTest=data;
              data.at_ms=10000;data.edges[0].samples[0].observed_at_ms=9000;
              DacpTopology.render(data,()=>{});
              data.at_ms=11000;data.edges[0].samples[0].observed_at_ms=10000;data.edges[0].samples[0].bytes+=1024;
              DacpTopology.render(data,()=>{});
            }""")
            assert page.locator('.flow-active').count() == 1
            assert '1 KiB/s' in page.locator('.wire-rate').first.text_content()
            page.evaluate("rateTest.edges[0].samples[0].key='retry2';DacpTopology.render(rateTest,()=>{})")
            assert page.locator('.flow-active').count() == 0
            assert 'Rate —' in page.locator('.wire-rate').first.text_content()
            page.evaluate('refreshNodes()')

            assert len(dataset_reads) == 0
            cards = page.locator('.directory-node')
            page.locator('.network-node').first.click()
            page.wait_for_selector('.directory-body tbody tr')
            assert len(dataset_reads) == 1 and page.url == origin+'/nodes'
            assert '<img' in cards.nth(0).locator('pre').inner_text()
            assert cards.nth(0).locator('.directory-body img').count() == 0
            cards.nth(0).locator('.directory-heading').click()
            cards.nth(0).locator('.directory-heading').click()
            assert len(dataset_reads) == 1
            cards.nth(0).get_by_role('button',name='Reload').click()
            page.wait_for_function('() => !document.querySelector(".directory-body").hasAttribute("aria-busy")')
            assert len(dataset_reads) == 2
            cards.nth(1).locator('.directory-heading').click()
            page.wait_for_selector('.directory-body .empty')
            cards.nth(2).locator('.directory-heading').click()
            retry = cards.nth(2).get_by_role('button',name='Retry',exact=True)
            retry.wait_for()
            fail_node[0] = False
            retry.click()
            cards.nth(2).locator('tbody tr').wait_for()
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            page.get_by_role('button',name='Refresh nodes').click()
            page.wait_for_selector('.directory-heading')
            assert page.locator('.directory-heading[aria-expanded="true"]').count() == 0
            assert not errors, errors
            catalog_reads=[]
            def catalog_response(route):
                catalog_reads.append(route.request.url)
                route.fulfill(content_type='application/json',body=json.dumps(dict(complete=False,unavailable_sources=1,observed_at_ms=1,datasets=[dict(name='ds-abcd-2345',description='{"summary":"<script>test</script>"}',uri='dacp://union/ds-abcd-2345')])))
            page.route('**/datasets/entries',catalog_response)
            page.goto(origin+'/datasets')
            page.wait_for_selector('#catalog-rows pre')
            assert page.locator('.directory-heading').count() == 0
            assert 'ds-abcd-2345' in page.locator('#catalog-rows').inner_text()
            assert 'Incomplete catalog' in page.locator('#catalog-error').inner_text()
            assert page.locator('#catalog-rows script').count() == 0
            page.locator('#catalog-search').fill('no-match')
            assert 'No matching' in page.locator('#catalog-rows').inner_text()
            page.locator('#catalog-search').fill('abcd')
            assert page.locator('#catalog-rows pre').count() == 1
            page.get_by_role('button',name='Refresh catalog').click()
            page.wait_for_function('() => !document.querySelector("#refresh-catalog").disabled')
            assert len(catalog_reads)==2
            assert page.evaluate('document.documentElement.scrollWidth <= innerWidth')
            assert not errors, errors
            browser.close()
            print('G6 browser contracts passed: grouping, clicks, zoom, stable updates, collapse, safe text, mobile and offline assets')
    finally:
        server.shutdown()
        server.server_close()


if __name__ == '__main__':
    main()

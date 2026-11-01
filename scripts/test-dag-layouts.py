#!/usr/bin/env python3
"""Browser regression for running Union/node DAG dashboards (requires Playwright)."""
import argparse
from playwright.sync_api import sync_playwright
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--chromium', help='Optional Chromium executable path')
parser.add_argument('--dag-script', help='Optional local renderer to test before deployment')
parser.add_argument('--urls', nargs='+', default=['http://127.0.0.1:19880','http://127.0.0.1:19861'])
args=parser.parse_args()
with sync_playwright() as p:
 b=p.chromium.launch(executable_path=args.chromium,headless=True)
 for url in args.urls:
  page=b.new_page(viewport={'width':1600,'height':1100});errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
  if args.dag_script:page.route('**/dag.js',lambda route:route.fulfill(path=args.dag_script,content_type='text/javascript'))
  page.goto(url);selector='#jobs .job-link' if page.locator('#jobs').count() else '#history .job-link'
  if '/job?' not in url:
   if page.locator('#job-view').count():page.locator('#job-view').select_option('history')
   page.locator(selector).first.click()
  page.wait_for_function('() => dagViewer.ready');page.evaluate('dagViewer.pending')
  for layout,direction in [('dependency','vertical'),('swimlane','vertical'),('swimlane','horizontal'),('dependency','horizontal')]:
   mode=layout+':'+direction
   page.locator(f'[data-dag-layout={layout}]').click();page.locator(f'[data-dag-direction={direction}]').click();page.evaluate('dagViewer.pending');assert page.locator('#dag-error').is_hidden(),page.locator('#dag-error').inner_text()
   assert page.evaluate('dagViewer.renderedLayout')==mode
   positions=page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])');assert all(isinstance(n[1],(float,int)) and isinstance(n[2],(float,int)) for n in positions),positions
   assert page.locator(f'[data-dag-layout={layout}]').get_attribute('aria-pressed')=='true'
   assert page.locator(f'[data-dag-direction={direction}]').get_attribute('aria-pressed')=='true'
   geometry=page.evaluate('dagViewer.graph.getData()')
   nodes={n['id']:n for n in geometry['nodes']}
   axis='y' if direction=='vertical' else 'x'
   for edge in geometry['edges']:
    source,target=nodes[edge['source']],nodes[edge['target']]
    if layout=='swimlane' or source['combo']==target['combo']:
     assert target['style'][axis]>source['style'][axis],(mode,edge)
   if layout=='swimlane':
    # Compare the rendered root frames, including nested execution units,
    # rather than only the cards directly owned by each server.
    bounds=page.evaluate('dagViewer.graph.getComboData().filter(c=>!c.combo).map(c=>dagViewer.graph.getElementRenderBounds(c.id))')
    cross=0 if direction=='vertical' else 1
    lanes=sorted((box['min'][cross],box['max'][cross]) for box in bounds)
    assert all(a[1]<b[0] for a,b in zip(lanes,lanes[1:])),lanes
   # Physical regions share their cross-flow dimension, separately by type.
   for kind in ('server','process'):
    sizes=page.evaluate("kind => dagViewer.graph.getComboData().filter(c=>c.data?.kind===kind).map(c=>{const b=dagViewer.graph.getElementRenderBounds(c.id);return b.max["+str(0 if direction=='vertical' else 1)+"]-b.min["+str(0 if direction=='vertical' else 1)+"];})",kind)
    assert not sizes or max(sizes)-min(sizes)<1,(mode,kind,sizes)
   original=page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])')
   page.wait_for_timeout(2500);page.evaluate('dagViewer.pending');assert page.evaluate('dagViewer.renderedLayout')==mode
   assert page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])')==original
   before=page.evaluate('dagViewer.graph.getZoom()');page.locator('#dag-zoom-in').click();page.evaluate('dagViewer.pending');assert page.evaluate('dagViewer.graph.getZoom()')>before
   page.locator('#dag-fit').click();page.evaluate('dagViewer.pending')
   print(url,mode,'OK',flush=True)
  assert not errors,errors
  page.close()
 b.close()

#!/usr/bin/env python3
"""Check logical and physical plan scripts, graphs and manual-refresh-preserved view selection."""
import argparse,json
from urllib.request import urlopen
from playwright.sync_api import sync_playwright,expect
p=argparse.ArgumentParser(description=__doc__);p.add_argument('--url',default='http://localhost:19880');p.add_argument('--job-id');p.add_argument('--node',action='store_true');p.add_argument('--chromium',required=True);a=p.parse_args()
if not a.job_id:
 with urlopen(a.url+'/api/history') as response:a.job_id=json.load(response)['jobs'][0]['job_id']
with sync_playwright() as pw:
 browser=pw.chromium.launch(executable_path=a.chromium,args=['--no-sandbox'])
 page=browser.new_page(viewport={'width':1440,'height':1000});errors=[]
 page.on('pageerror',lambda e:errors.append(str(e)))
 page.goto(a.url+'/job?id='+a.job_id)
 expect(page.locator('#physical-plan')).to_be_visible();expect(page.locator('#dag canvas').first).to_be_visible(timeout=20000)
 page.wait_for_function('dagViewer.ready && dagViewer.graph.getData().combos.length > 0')
 physical_count=page.evaluate('dagViewer.graph.getData().nodes.length')
 page.locator('#logical-plan-tab').click();expect(page.locator('#logical-plan')).to_be_visible();expect(page.locator('#physical-plan')).to_be_hidden()
 page.wait_for_function('dagViewer.ready && dagViewer.job.endsWith(":logical") && dagViewer.graph.getData().combos.length === 0')
 assert page.evaluate('dagViewer.graph.getData().nodes.length')==physical_count
 page.locator('[data-plan-view="logical:script"]').click();expect(page.locator('#job-script')).to_be_visible();expect(page.locator('#dag')).to_be_hidden()
 text=page.locator('#job-script').inner_text();assert 'tasks:' in text and 'node:' not in text
 page.locator('#refresh').click();page.wait_for_timeout(500);expect(page.locator('#job-script')).to_be_visible();expect(page.locator('#logical-plan-tab')).to_have_attribute('aria-selected','true')
 page.locator('#physical-plan-tab').click();expect(page.locator('#dag')).to_be_visible()
 page.wait_for_function('dagViewer.ready && !dagViewer.job.endsWith(":logical") && dagViewer.graph.getData().combos.length > 0')
 page.locator('[data-plan-view="physical:script"]').click();expect(page.locator('#resolved-job-script')).to_be_visible();assert page.locator('#resolved-job-script').inner_text().strip()
 if not a.node:assert 'node:' in page.locator('#resolved-job-script').inner_text()
 page.locator('#logical-plan-tab').click();expect(page.locator('#job-script')).to_be_visible()
 page.locator('[data-plan-view="logical:dag"]').click();expect(page.locator('#dag')).to_be_visible()
 page.locator('#logical-plan-tab').press('ArrowRight')
 expect(page.locator('#physical-plan-tab')).to_have_attribute('aria-selected','true');expect(page.locator('#resolved-job-script')).to_be_visible()
 expect(page.locator('#error')).to_be_hidden();assert not errors,errors
 browser.close()
print('PASS distinct logical/physical graphs, YAML scripts, independent view state, keyboard navigation and manual refresh')

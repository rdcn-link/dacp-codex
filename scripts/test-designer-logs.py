#!/usr/bin/env python3
"""Verify log history, severities, latest status, filtering and request error capture."""
import argparse
from playwright.sync_api import sync_playwright, expect
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://localhost:8840')
parser.add_argument('--chromium',required=True)
args=parser.parse_args()
with sync_playwright() as p:
 browser=p.chromium.launch(executable_path=args.chromium,args=['--no-sandbox','--disable-dev-shm-usage','--disable-gpu'])
 page=browser.new_page(viewport={'width':1450,'height':950})
 errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
 page.route('**/api/log-probe',lambda r:r.fulfill(status=502,json={'error':'Worker startup timed out'}))
 page.goto(args.url)
 expect(page.locator('#connection')).to_contain_text('Connected',timeout=25000)
 expect(page.locator('#repository-state')).to_contain_text('published',timeout=25000)
 page.locator('[data-tab="logs"]').dispatch_event('click')
 expect(page.locator('#log-output')).to_be_visible()
 page.locator('#log-clear').dispatch_event('click')
 page.evaluate("logMessage('First message','info');logMessage('Incomplete parameter','warning','Schema');logMessage('Latest error','error')")
 expect(page.locator('#log-output .log-entry')).to_have_count(3)
 expect(page.locator('#notice')).to_have_text('Latest error')
 expect(page.locator('#log-output .log-message').first).to_have_text('Latest error')
 expect(page.locator('#notice')).to_have_class('error')
 for level in ['info','warning','error']:
  page.locator('#log-filter').select_option(level)
  expect(page.locator('#log-output .log-entry')).to_have_count(1)
  expect(page.locator('#log-output .log-entry')).to_have_class('log-entry log-'+level)
 page.locator('#log-filter').select_option('all')
 page.evaluate("guard(()=>api('log-probe',{}))()")
 expect(page.locator('#notice')).to_have_text('log-probe: Worker startup timed out')
 expect(page.locator('#log-output .log-error')).to_have_count(2)
 expect(page.locator('#log-output')).to_contain_text('Request started: log-probe')
 # Treat returned error messages as text, never as HTML.
 page.evaluate("logMessage('<img src=x onerror=alert(1)>','warning')")
 expect(page.locator('#log-output img')).to_have_count(0)
 expect(page.locator('#notice')).to_have_text('<img src=x onerror=alert(1)>')
 assert page.locator('#notice').evaluate('e=>getComputedStyle(e).whiteSpace')=='nowrap'
 page.evaluate("for(let i=0;i<70;i++)logMessage('Line '+i)")
 page.locator('#log-follow').uncheck(force=True)
 page.locator('#log-output').evaluate('e=>e.scrollTop=30')
 anchor=page.locator('#log-output').evaluate('''e=>{const row=[...e.children].find(r=>r.offsetTop+r.offsetHeight>e.scrollTop);return {id:row.dataset.logId,offset:row.offsetTop-e.scrollTop}}''')
 page.evaluate("logMessage('Keep reading position')")
 after=page.locator('#log-output').evaluate('''(e,id)=>{const row=e.querySelector('[data-log-id="'+id+'"]');return row.offsetTop-e.scrollTop}''',anchor['id'])
 assert abs(after-anchor['offset'])<2
 page.locator('#log-follow').check(force=True)
 assert page.locator('#log-output').evaluate('e=>e.scrollTop')==0
 expect(page.locator('#log-output .log-message').first).to_have_text('Keep reading position')
 page.locator('#log-clear').dispatch_event('click')
 expect(page.locator('#log-output .log-entry')).to_have_count(0)
 expect(page.locator('#log-count')).to_have_text('0')
 assert not errors,errors
 browser.close()
 print('Log history, severities, filtering, latest status, error capture, text safety and scroll preservation passed')

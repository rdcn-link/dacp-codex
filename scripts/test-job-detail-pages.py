#!/usr/bin/env python3
"""Check separate, directly addressable job details on Union and node dashboards."""
import argparse
from playwright.sync_api import sync_playwright, expect
p=argparse.ArgumentParser(description=__doc__)
p.add_argument('--chromium',required=True)
p.add_argument('--urls',nargs='+',default=['http://localhost:19880','http://localhost:19861'])
a=p.parse_args()
with sync_playwright() as pw:
 browser=pw.chromium.launch(executable_path=a.chromium,args=['--no-sandbox'])
 for url in a.urls:
  page=browser.new_page();errors=[]
  page.on('pageerror',lambda e:errors.append(str(e)))
  page.goto(url)
  if page.locator('#job-view').count():page.locator('#job-view').select_option('history')
  expect(page.locator('.job-link').first).to_be_visible(timeout=20000)
  assert page.locator('#detail').count()==0
  link=page.locator('.job-link').first;href=link.get_attribute('href');assert href.startswith('/job?id=')
  link.click();page.wait_for_url('**/job?id=*');expect(page.locator('#detail')).to_be_visible()
  expect(page.locator('#dag canvas').first).to_be_visible(timeout=20000)
  expect(page.locator('#error')).to_be_hidden();calls=[];page.on('request',lambda request:calls.append(request.url) if '/api/' in request.url else None);page.wait_for_timeout(2300)
  assert not calls,'Details must not poll automatically'
  page.locator('#refresh').click();page.wait_for_timeout(500);assert calls,'Manual refresh must fetch current details'
  expect(page.locator('#error')).to_be_hidden();page.reload()
  expect(page.locator('#dag canvas').first).to_be_visible(timeout=20000)
  page.get_by_role('link',name='Back to jobs',exact=False).click();expect(page.locator('#jobs, #history')).to_be_visible()
  assert not errors,errors
  print('PASS',url,href);page.close()
 browser.close()

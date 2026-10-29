#!/usr/bin/env python3
"""Browser checks for distinct left resource tabs and retained panel state."""
import argparse
from playwright.sync_api import sync_playwright, expect

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--url',default='http://localhost:8840')
parser.add_argument('--chromium',required=True)
args=parser.parse_args()
with sync_playwright() as p:
    browser=p.chromium.launch(executable_path=args.chromium,args=['--no-sandbox','--disable-dev-shm-usage','--disable-gpu'])
    page=browser.new_page(viewport={'width':1400,'height':900})
    errors=[]
    page.on('pageerror',lambda error:errors.append(str(error)))
    dataset_requests=[]
    page.route('**/api/catalogs',lambda route:route.fulfill(json=[{'name':'In-context catalog','uri':'context:/'}]))
    page.route('**/api/datasets',lambda route:(dataset_requests.append(True),route.fulfill(json={'complete':True,'unavailable_sources':0,'datasets':[{'name':'Climate','uri':'dacp://localhost:19860/climate','description':'{}'}]})))
    page.route('**/api/items',lambda route:route.fulfill(json={'items':[{'path':'/observations','kind':'item','views':['records']}]}))
    page.goto(args.url)
    expect(page.locator('#repository-state')).to_contain_text('published',timeout=25000)
    expect(page.locator('#connection')).to_contain_text('Connected',timeout=25000)
    expect(page.locator('.library-tabs [role=tab]')).to_have_count(2)
    expect(page.locator('.library-tabs svg')).to_have_count(2)
    expect(page.locator('.library-tabs')).to_have_attribute('aria-orientation','vertical')
    boxes=[page.locator('#library-'+name+'-tab').bounding_box() for name in ['operators','datasets']]
    assert boxes[0]['y']+boxes[0]['height']<=boxes[1]['y']
    assert len({box['x'] for box in boxes})==1
    assert page.locator('#library-operators-panel').bounding_box()['x']>boxes[0]['x']+boxes[0]['width']
    expect(page.locator('#library-operators-tab')).to_have_attribute('aria-selected','true')
    expect(page.locator('#library-operators-panel')).to_be_visible()
    expect(page.locator('#library-datasets-panel')).to_be_hidden()
    assert page.locator('#library-operators-tab').evaluate('e=>getComputedStyle(e).borderTopWidth')=='0px'
    page.locator('#search').fill('piflow_filter')
    expect(page.locator('#operators .operator')).to_have_count(2)
    page.locator('#library-operators-tab').press('End')
    expect(page.locator('#library-operators-panel')).to_be_hidden()
    expect(page.locator('#library-datasets-tab')).to_be_focused()
    expect(page.locator('#library-datasets-panel')).to_be_visible()
    page.get_by_role('button',name='In-context catalog',exact=True).click()
    expect(page.get_by_role('button',name='Climate',exact=True)).to_be_visible()
    assert len(dataset_requests)==1
    expect(page.locator('.dataset-button svg')).to_have_count(2)
    assert page.get_by_role('button',name='Climate',exact=True).evaluate('e=>getComputedStyle(e).borderTopWidth')=='0px'
    expect(page.get_by_role('button',name='Climate',exact=True)).to_have_attribute('aria-expanded','false')
    page.get_by_role('button',name='Climate',exact=True).click()
    expect(page.get_by_role('button',name='Climate',exact=True)).to_have_attribute('aria-expanded','true')
    expect(page.locator('#datasets .item')).to_have_count(1)
    page.locator('#library-operators-tab').click()
    expect(page.locator('#search')).to_have_value('piflow_filter')
    expect(page.locator('#operators .operator')).to_have_count(2)
    page.locator('#search').fill('')
    page.locator('.operator-category').first.locator('summary').click()
    expect(page.locator('.operator-category').first).not_to_have_attribute('open','')
    page.locator('#library-operators-panel').evaluate("e=>{e.style.maxHeight='120px';e.scrollTop=80}")
    scroll=page.locator('#library-operators-panel').evaluate('e=>e.scrollTop')
    assert scroll>0
    page.locator('#library-datasets-tab').click()
    expect(page.locator('#datasets .item')).to_have_count(1)
    assert len(dataset_requests)==1
    page.locator('#library-datasets-tab').press('Home')
    expect(page.locator('#library-operators-tab')).to_be_focused()
    assert page.locator('#library-operators-panel').evaluate('e=>e.scrollTop')==scroll
    expect(page.locator('.operator-category').first).not_to_have_attribute('open','')
    page.locator('#library-operators-tab').press('ArrowUp')
    expect(page.locator('#library-datasets-tab')).to_be_focused()
    page.locator('#library-datasets-tab').press('ArrowDown')
    expect(page.locator('#library-operators-tab')).to_be_focused()
    page.locator('#library-operators-panel').evaluate("e=>{e.style.maxHeight='';e.scrollTop=0}")
    page.locator('#search').fill('read_item')
    page.locator('[data-operator="read_item@1.0"]').drag_to(page.locator('#canvas'),target_position={'x':110,'y':110})
    expect(page.locator('.card')).to_have_count(1)
    assert page.evaluate('!Object.hasOwn(job.tasks[0],"node")')
    page.locator('#library-datasets-tab').click()
    expect(page.locator('#datasets-refresh')).to_have_count(0)
    assert len(dataset_requests)==1
    assert not errors,errors
    browser.close()
print('PASS two resource tabs, distinct styling, keyboard navigation and retained search, expansion and scrolling')

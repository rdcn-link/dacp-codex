#!/usr/bin/env python3
"""Verify parameter-driven port schemas, downstream updates and stale-response rejection."""
import argparse, asyncio, json
from playwright.async_api import async_playwright, expect

async def main():
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--url',default='http://localhost:8840')
 parser.add_argument('--chromium',required=True)
 args=parser.parse_args()
 async with async_playwright() as p:
  browser=await p.chromium.launch(executable_path=args.chromium,args=['--no-sandbox','--disable-dev-shm-usage','--disable-gpu'])
  page=await browser.new_page(viewport={'width':1450,'height':950})
  errors=[];requests=[];old_started=asyncio.Event();release_old=asyncio.Event()
  page.on('pageerror',lambda e:errors.append(str(e)))
  async def infer(route):
   n=route.request.post_data_json;draft=json.loads(n['job_yaml']);uri=draft['tasks'][0]['params']['uri'];
   if not n.get('preview'):requests.append(uri)
   if uri.endswith('/old'):
    old_started.set();await release_old.wait()
   name=uri.rsplit('/',1)[-1]
   descriptor={'representations':['records'],'formats':['arrow'],'certainty':'known','schema_json':{'fields':[{'name':name,'type':'utf8','nullable':True}]}}
   report={'tasks':{t['id']:{'outputs':{'result':descriptor},'diagnostics':[]} for t in draft['tasks']},'outputs':{'result':descriptor}}
   if route.request.url.endswith('/infer_stream'):
    events=[{'event':'step','step':id,'result':result} for id,result in report['tasks'].items()]+[{'event':'complete','report':report}]
    await route.fulfill(content_type='application/x-ndjson',body='\n'.join(json.dumps(e) for e in events)+'\n')
   else:await route.fulfill(json=report)
  await page.route('**/api/infer',infer)
  await page.route('**/api/infer_stream',infer)
  await page.goto(args.url)
  await expect(page.locator('#connection')).to_contain_text('Connected',timeout=25000)
  await expect(page.locator('#repository-state')).to_contain_text('published',timeout=25000)
  draft={'name':'Dynamic schemas','tasks':[{'id':'read','operator':'read_item@1.0','inputs':{},'params':{'uri':'dacp://localhost:19860/example/initial','view':'records'}},{'id':'filter','operator':'piflow_filter@1.0','inputs':{'source':'read'},'params':{'condition':'true'}}],'outputs':{'result':'filter'}}
  await page.evaluate('(draft)=>loadDocument(JSON.stringify(draft))',draft)
  await expect(page.locator('[data-schema-port="read:result"]')).to_contain_text('initial',timeout=15000)
  await expect(page.locator('[data-schema-port="filter:result"]')).to_contain_text('initial')
  table=page.locator('[data-schema-port="read:result"] .port-schema-table')
  await expect(table.locator('th')).to_have_text('initial')
  await expect(table.locator('td')).to_have_text('string?')
  compact=await table.evaluate('(e)=>({font:getComputedStyle(e).fontSize,padding:getComputedStyle(e.querySelector("td")).padding})')
  assert compact=={'font':'8px','padding':'1px 3px'},compact

  await page.wait_for_function('()=>!inferenceRunning',polling=100)
  count=len(requests)
  await page.evaluate("selected='read';selectedSteps=new Set(['read']);setInspector(true);render();inspectorTab('outputs');inspectorTab('parameters');job.name='Renamed';job.tasks[0].title='New title';positions.read={x:400,y:300};invalidate();render()")
  await page.wait_for_timeout(1000)
  assert len(requests)==count,requests
  icons=await page.locator('#properties-tab path, #parameters-tab path').evaluate_all('(paths)=>paths.map(p=>p.getAttribute("d"))')
  assert len(set(icons))==2,icons
  await page.evaluate("selected='read';selectedSteps=new Set(['read']);setInspector(true);render();inspectorTab('parameters')")
  tabs=await page.locator('.inspector-tabs').bounding_box();assert tabs['height']<=36,tabs
  value=page.get_by_role('group',name='uri',exact=True).get_by_label('Parameter value',exact=True)
  await value.fill('dacp://localhost:19860/example/old')
  await asyncio.wait_for(old_started.wait(),15)
  await value.fill('dacp://localhost:19860/example/latest')
  release_old.set()
  for step in ('read','filter'):
   await expect(page.locator(f'[data-schema-port="{step}:result"]')).to_contain_text('latest',timeout=15000)
   await expect(page.locator(f'[data-schema-port="{step}:result"]')).not_to_contain_text('old')
  # Valid edits save automatically without replacing the focused form.
  assert await value.input_value()=='dacp://localhost:19860/example/latest'
  assert await page.evaluate('job.tasks[0].params.uri')=='dacp://localhost:19860/example/latest'
  assert requests[-2:]==['dacp://localhost:19860/example/old','dacp://localhost:19860/example/latest'],requests
  await page.wait_for_function('()=>!inferenceRunning',polling=100)
  await page.evaluate("report={tasks:{read:{outputs:{},diagnostics:[{severity:'error',message:'A very long detailed inference failure that belongs in Outputs only'}]}}};refreshPortSchemas()")
  error_box=page.locator('[data-schema-port="read:result"]')
  await expect(error_box).to_have_text('!Inference error')
  await expect(error_box).not_to_contain_text('very long')
  await page.evaluate("report={tasks:{read:{outputs:{result:{representations:['records'],certainty:'unknown'}},diagnostics:[]}}};refreshPortSchemas()")
  await expect(error_box).to_have_text('?Schema unknown')
  # Records samples render as a bounded table without interpreting HTML values.
  await page.evaluate("report.tasks.read.outputs.result={representations:['records'],certainty:'known',schema_json:{fields:[{name:'value',type:'utf8'}]},sample:{representation:'records',format:'json',rows:[{value:'<script>unsafe</script>'},{value:null}]}};selected='read';selectedSteps=new Set(['read']);setInspector(true);render();inspectorTab('outputs')")
  await expect(page.locator('.output-sample-table th')).to_have_text('value')
  await expect(page.locator('.output-sample-table tbody tr')).to_have_count(2)
  await expect(page.locator('.output-sample-table td').first).to_have_text('<script>unsafe</script>')
  assert await page.locator('#save-step').count()==0
  # Duplication retains settings but not input bindings, and is undoable.
  await page.evaluate("selected='filter';selectedSteps=new Set(['filter']);render()")
  await page.locator('#duplicate-step').dispatch_event('click')
  copy=await page.evaluate('job.tasks.at(-1)')
  assert copy['id'] not in ('read','filter') and copy['operator']=='piflow_filter@1.0' and copy['inputs']=={},copy
  await page.locator('#undo').dispatch_event('click')
  assert await page.evaluate('job.tasks.length')==2
  # Move the existing multi-selection using a real pointer drag.
  await page.evaluate("selectedSteps=new Set(['read','filter']);selected=null;outletSelected=false;positions={read:{x:80,y:120},filter:{x:380,y:120}};nodeAreas={};setInspector(false);render();document.getElementById('canvas').scrollTo(0,0)")
  before=await page.evaluate('JSON.parse(JSON.stringify(positions))')
  head=await page.locator('[data-task="read"] .card-head').bounding_box()
  x,y=head['x']+25,head['y']+12
  await page.mouse.move(x,y);await page.mouse.down();await page.mouse.move(x+70,y+55,steps=8);await page.mouse.up()
  after=await page.evaluate('positions')
  for axis in ('x','y'):assert abs((after['read'][axis]-before['read'][axis])-(after['filter'][axis]-before['filter'][axis]))<1
  assert after['read']!=before['read'],after
  # Validation reports unconnected and disconnected steps in a separate panel.
  await page.evaluate("job.outputs={};invalidate()")
  await page.locator('#validate').dispatch_event('click')
  await expect(page.locator('#validation-results')).to_contain_text('Outlet must connect')
  await expect(page.locator('#validation-results')).to_contain_text('No dataflow path')
  assert not errors,errors
  await browser.close()
  print('Dynamic port schemas, downstream propagation, stale response rejection, automatic parameter saving and compact tables passed')

asyncio.run(main())

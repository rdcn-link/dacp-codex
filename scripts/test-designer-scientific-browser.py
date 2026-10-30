#!/usr/bin/env python3
"""Exercise scientific source selection, per-field forms and compact previews without live services."""
import json, mimetypes, argparse
from pathlib import Path
from playwright.sync_api import sync_playwright, expect

parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--chromium');args=parser.parse_args()
root=Path(__file__).resolve().parents[1]
web=root/'xstream-designer/src/main/resources/web'
integer={'type':'integer','minimum':0}
views={
 'array':{'selection':{'type':'object','properties':{k:{'type':'array','items':integer} for k in ('origin','shape','stride')},'additionalProperties':False}},
 'image':{'selection':{'type':'object','properties':{k:integer for k in ('x','y','width','height')},'additionalProperties':False}},
 'sequence':{'selection':{'type':'object','properties':{'ids':{'type':'array','items':{'type':'string'}},'offset':integer,'length':integer},'additionalProperties':False}}}

def route(req):
 path=req.request.url.split('designer.test',1)[1]
 if path.startswith('/api/'):
  name=path.rsplit('/',1)[1];n=req.request.post_data_json if req.request.method=='POST' else {}
  if name=='config':data={'chat_provider':'builtin','generator':{'name':'XStream Designer','version':'test'}}
  elif name=='views':data=views
  elif name=='operators':data=[]
  elif name=='catalogs':data=[{'name':'In-context catalog','uri':'context:/'}]
  elif name=='datasets':data={'complete':True,'datasets':[{'name':'science','uri':'context:/science'}]}
  elif name=='items':data={'items':[{'path':'/genome','kind':'folder','views':['bytes'],'format':'fasta'}]} if n['path']=='/' else {'items':[{'path':'/genome/sequences','kind':'data','views':['sequence','bytes'],'format':'fasta'}]}
  elif name=='document':data={'job':json.loads(n['job_yaml']),'job_yaml':n['job_yaml'],'variables_yaml':n.get('variables_yaml','{}')}
  elif name in ('infer','infer_stream'):
   job=json.loads(n['job_yaml']);tasks={}
   for t in job['tasks']:
    view=t['params'].get('view','sequence');structure={'sequence':{'view':'sequence','alphabet':'dna'},'image':{'view':'image','width':12,'height':8,'channels':1,'data_type':'uint8'},'array':{'view':'array','data_type':'float32','dimensions':[{'name':'time','length':4},{'name':'lat','length':180}]}}.get(view)
    d={'representations':[view],'formats':['fasta'],'certainty':'known','structure':structure}
    if n.get('preview'):d['sample']={'representation':view,'format':'fasta','rows':[{'id':'chr1','offset':2,'symbols':'GTAC','last':True}],'truncated':False}
    tasks[t['id']]={'outputs':{'result':d},'diagnostics':[]}
   data={'tasks':tasks,'outputs':next(iter(tasks.values()),{}).get('outputs',{})}
   if name=='infer_stream':
    events=[{'event':'step','step':k,'result':v} for k,v in tasks.items()]+[{'event':'complete','report':data}]
    req.fulfill(content_type='application/x-ndjson',body='\n'.join(json.dumps(x) for x in events)+'\n');return
  else:data={}
  req.fulfill(json=data);return
 file=web/('index.html' if path=='/' else path.lstrip('/'))
 if not file.is_file():file=root/'assets'/path.lstrip('/')
 if file.is_file():req.fulfill(body=file.read_bytes(),content_type=mimetypes.guess_type(file.name)[0] or 'application/octet-stream')
 else:req.fulfill(status=404,body='missing')

with sync_playwright() as p:
 browser=p.chromium.launch(executable_path=args.chromium,args=['--no-sandbox','--disable-dev-shm-usage'])
 page=browser.new_page(viewport={'width':1500,'height':1000});errors=[]
 page.on('pageerror',lambda e:errors.append(str(e)));page.route('http://designer.test/**',route)
 page.goto('http://designer.test/')
 expect(page.locator('#connection')).to_have_text('Connected')
 page.locator('#library-datasets-tab').click()
 page.get_by_role('button',name='In-context catalog',exact=True).click()
 page.get_by_role('button',name='science',exact=True).click()
 expect(page.locator('.item-label')).to_have_text('genome')
 page.locator('.item[data-kind=folder]').click()
 expect(page.get_by_role('toolbar',name='Item actions')).to_be_visible()
 expect(page.get_by_role('dialog',name='Item views')).to_have_count(0)
 expect(page.get_by_role('button',name='Copy path',exact=True)).to_have_attribute('title','Copy path: /science/genome')
 page.evaluate("document.execCommand=()=>{window.copiedItemPath=document.activeElement.value;return true}")
 page.get_by_role('button',name='Copy path',exact=True).click()
 assert page.evaluate('window.copiedItemPath')=='/science/genome'
 page.keyboard.press('Escape')
 expect(page.get_by_role('toolbar',name='Item actions')).to_have_count(0)
 assert page.evaluate('job.tasks.length')==0
 expect(page.locator('.item[data-kind=item] .item-label')).to_have_text('sequences')
 page.locator('.item[data-kind=item]').click()
 expect(page.get_by_role('button',name='Use bytes view',exact=True)).to_be_visible()
 assert page.evaluate('job.tasks.length')==0
 page.get_by_role('button',name='Use sequence view',exact=True).click()
 assert page.evaluate('job.tasks[0].params')=={'uri':'context:/science/genome/sequences','view':'sequence'}
 assert page.evaluate('executionDocument().tasks[0].params.view')=='sequence'
 page.locator('.item[data-kind=item]').click()
 page.get_by_role('button',name='Use bytes view',exact=True).click()
 assert page.evaluate('job.tasks.length')==1
 assert page.evaluate('executionDocument().tasks[0].params.view')=='bytes'
 page.locator('.item[data-kind=item]').click()
 page.get_by_role('button',name='Use sequence view',exact=True).click()
 assert page.evaluate('job.tasks.length')==1
 expect(page.locator('[data-schema-port="step_1:result"]')).to_contain_text('dna')
 assert page.locator('#datasets .item-capability').count()==0
 page.evaluate("job.outputs={result:'step_1'};selected='step_1';setInspector(true);render();inspectorTab('parameters')")
 expect(page.get_by_role('group',name='selection',exact=True)).to_be_visible()
 length=page.locator('[data-parameter-path="params.selection.length"]')
 # Enable selection and edit its range through individual controls.
 page.get_by_label('Set selection',exact=True).check()
 length.get_by_label('Set length',exact=True).check()
 length.get_by_label('Parameter value',exact=True).fill('4')
 expect(page.locator('#step-save-status')).to_have_text('')
 assert page.evaluate('job.tasks[0].params.selection.length')==4
 page.get_by_role('group',name='view',exact=True).get_by_label('Parameter value',exact=True).select_option(label='"image"')
 expect(page.get_by_label('Set selection',exact=True)).not_to_be_checked()
 page.get_by_label('Set selection',exact=True).check()
 expect(page.locator('[data-parameter-path="params.selection.width"]')).to_be_visible()
 assert page.evaluate('job.tasks[0].params.selection') == {}, page.evaluate('job.tasks[0].params')
 page.get_by_role('group',name='view',exact=True).get_by_label('Parameter value',exact=True).select_option(label='"array"')
 page.get_by_label('Set selection',exact=True).check()
 expect(page.locator('[data-parameter-path="params.selection.stride"]')).to_be_visible()
 expect(page.locator('[data-schema-port="step_1:result"]')).to_contain_text('lat')
 page.evaluate("inspectorTab('outputs')")
 expect(page.locator('.output-sample-table')).to_contain_text('chr1')
 assert not errors,errors
 browser.close()
print('Scientific designer browser checks passed')

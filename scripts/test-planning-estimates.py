import asyncio,json,argparse
from pathlib import Path
from playwright.async_api import async_playwright
ROOT=Path(__file__).resolve().parents[1]
parser=argparse.ArgumentParser(description='Verify physical planning estimates and actual-unit precedence in the DAG')
parser.add_argument('--chromium',required=True)
args=parser.parse_args()
async def main():
 async with async_playwright() as p:
  browser=await p.chromium.launch(executable_path=args.chromium,args=['--no-sandbox']);page=await browser.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
  await page.add_script_tag(content=(ROOT/'dacp-union-server/src/main/resources/web/dag.js').read_text())
  result=await page.evaluate('''()=>{
   const steps=[{id:'a',operator:'op@1',node:'dacp://a:1',node_identity:'a',inputs:[]},{id:'b',operator:'op@1',node:'dacp://a:1',node_identity:'a',inputs:[{task:'a',port:'source'}]}];
   const job={job_id:'test',steps,nodes:{},outputs:{},planning:{steps:[{step:'a',output_bytes:100,compute_ms:10,confidence:'estimated'}],edges:[{from:'a',to:'b',mode:'arrow_in_process_candidate',estimated_bytes:100,estimated_transfer_ms:0}],execution_units:[{id:'unit',node:'dacp://a:1',steps:['a','b'],mode:'fusion_candidate',runtime:'test'}]}};
   const fmt={bytes:String,time:String,duration:String,labels:{},terminal:new Set()};
   const planned=DacpDag.buildData(job,new Map(),fmt);if(!planned.data.combos.some(c=>c.id.includes('unit:')))throw Error('missing planned units');
   if(!planned.details.get('task:a').lines.some(l=>l.includes('Estimate basis')))throw Error('missing estimate tooltip');
   const actual=DacpDag.buildData({...job,nodes:{a:{execution_units:[{id:'actual-a',steps:['a'],mode:'isolated_step'},{id:'actual-b',steps:['b'],mode:'isolated_step'}]}}},new Map(),fmt);
   if(actual.data.combos.some(c=>c.data.title==='Fusion candidate (estimated)'))throw Error('candidate overrides actual');
   return {planned:true,actual_priority:true};
  }''')
  assert not errors,errors
  print(json.dumps(result));await browser.close()
asyncio.run(main())

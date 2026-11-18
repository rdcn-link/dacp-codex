#!/usr/bin/env python3
"""Browser regression for running Union/node DAG dashboards (requires Playwright)."""
import argparse
from playwright.sync_api import sync_playwright
parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--chromium', help='Optional Chromium executable path')
parser.add_argument('--dag-script', help='Optional local renderer to test before deployment')
parser.add_argument('--urls', nargs='+', default=['http://127.0.0.1:19880','http://127.0.0.1:19861'])
def check_layouts(page,label):
    for layout,direction in [('dependency','vertical'),('swimlane','vertical'),('swimlane','horizontal'),('dependency','horizontal')]:
     mode=layout+':'+direction
     page.locator(f'[data-dag-layout={layout}]').click();page.locator(f'[data-dag-direction={direction}]').click();page.evaluate('dagViewer.pending');assert page.locator('#dag-error').is_hidden(),page.locator('#dag-error').inner_text()
     assert page.evaluate('dagViewer.renderedLayout')==mode
     positions=page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])');assert all(isinstance(n[1],(float,int)) and isinstance(n[2],(float,int)) for n in positions),positions
     assert page.locator(f'[data-dag-layout={layout}]').get_attribute('aria-pressed')=='true'
     assert page.locator(f'[data-dag-direction={direction}]').get_attribute('aria-pressed')=='true'
     geometry=page.evaluate('dagViewer.graph.getData()')
     nodes={n['id']:n for n in geometry['nodes']}
     # Every bound stream terminates at a named port; compact cards grow for fan-in.
     for node in nodes.values():
      if node['data'].get('kind')=='task':
       data=node['data']
       assert node['style']['size'][1]>=(70 if data.get('logical') else 118)+22*max(1,len(data['inputs']),len(data['outputs']))
       assert data['fill']=='#fff'
     for edge in geometry['edges']:
      source,target=nodes[edge['source']],nodes[edge['target']]
      if source['data'].get('kind')=='task':
       assert edge['style']['sourcePort'].removeprefix('output:') in source['data']['outputs']
      if target['data'].get('kind')=='task':
       assert edge['style']['targetPort'].removeprefix('input:') in target['data']['inputs']
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
     if layout=='dependency':
      # Parallel units retain dependency layers, rather than being flattened into
      # one row/column just to align their cross-axis origins.
      frames=page.evaluate("""() => {const graph=dagViewer.graph;return graph.getComboData().map(c=>({id:c.id,parent:c.combo,bounds:graph.getElementRenderBounds(c.id)}));}""")
      for i,a in enumerate(frames):
       for other in frames[i+1:]:
        if a.get('parent')!=other.get('parent'):continue
        x,y=a['bounds'],other['bounds']
        assert x['max'][0]<=y['min'][0] or y['max'][0]<=x['min'][0] or x['max'][1]<=y['min'][1] or y['max'][1]<=x['min'][1],(mode,a,other)
      roots={c['id']:c.get('combo') for c in geometry['combos']}
      def root(node):
       parent=node.get('combo')
       while roots.get(parent):parent=roots[parent]
       return parent
      for edge in geometry['edges']:
       source,target=nodes[edge['source']],nodes[edge['target']]
       if root(source)==root(target):assert target['style'][axis]>source['style'][axis],(mode,edge)
     if layout=='dependency':
      stability=page.evaluate("""async () => {
       const graph=dagViewer.graph;
       const extents=()=>graph.getComboData().map(c=>{const b=graph.getElementRenderBounds(c.id);return [c.id,b.max[0]-b.min[0],b.max[1]-b.min[1]]});
       const original=extents();
       await dagViewer.normalizeRegions();await dagViewer.normalizeRegions();
       const normalized=extents();
       const unit=graph.getComboData().find(c=>c.data?.kind==='process');
       if(unit){
        await graph.collapseElement(unit.id,{animation:false});await dagViewer.normalizeRegions();
        await graph.expandElement(unit.id,{animation:false});await dagViewer.normalizeRegions();
       }
       return {original,normalized,expanded:extents()};
      }""")
      for before,after,expanded in zip(stability['original'],stability['normalized'],stability['expanded']):
       assert before[0]==after[0]==expanded[0]
       assert all(abs(a-b)<1 for a,b in zip(before[1:],after[1:])),(mode,'padding accumulated',before,after)
       assert all(abs(a-b)<1 for a,b in zip(before[1:],expanded[1:])),(mode,'expand changed extents',before,expanded)
     collisions=page.evaluate("""() => {
      const graph=dagViewer.graph,result=[];
      const nodes=graph.getNodeData().map(n=>({id:n.id,box:graph.context.element.getElement(n.id).getShape('key').getBounds()}));
      for(const edge of graph.getEdgeData()){
       const d=graph.context.element.getElement(edge.id).getShape('key').attributes.d;
       const path=document.createElementNS('http://www.w3.org/2000/svg','path');path.setAttribute('d',Array.isArray(d)?d.map(segment=>segment.join(' ')).join(' '):d);
       const length=path.getTotalLength();
       for(const node of nodes){
        if(node.id===edge.source||node.id===edge.target)continue;
        for(let distance=2;distance<length;distance+=4){
         const p=path.getPointAtLength(distance),b=node.box;
         if(p.x>b.min[0]+1&&p.x<b.max[0]-1&&p.y>b.min[1]+1&&p.y<b.max[1]-1){result.push([edge.id,node.id]);break;}
        }
       }
      }
      return result;
     }""")
     assert not collisions,(mode,'connections intersect step cards',collisions)
     original=page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])')
     page.wait_for_timeout(2500);page.evaluate('dagViewer.pending');assert page.evaluate('dagViewer.renderedLayout')==mode
     assert page.evaluate('dagViewer.graph.getNodeData().map(n=>[n.id,n.style.x,n.style.y])')==original
     before=page.evaluate('dagViewer.graph.getZoom()');page.locator('#dag-zoom-in').click();page.evaluate('dagViewer.pending');assert page.evaluate('dagViewer.graph.getZoom()')>before
     page.locator('#dag-fit').click();page.evaluate('dagViewer.pending')
     print(label,mode,'OK',flush=True)

args=parser.parse_args()
with sync_playwright() as p:
 b=p.chromium.launch(executable_path=args.chromium,headless=True)
 for url in args.urls:
  page=b.new_page(viewport={'width':1600,'height':1100},bypass_csp=True);errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
  if args.dag_script:page.route('**/dag.js',lambda route:route.fulfill(path=args.dag_script,content_type='text/javascript'))
  page.goto(url);selector='#jobs .job-link' if page.locator('#jobs').count() else '#history .job-link'
  if '/job?' not in url:
   if page.locator('#job-view').count():page.locator('#job-view').select_option('history')
   page.locator(selector).first.click()
  page.wait_for_function('() => typeof dagViewer!=="undefined" && dagViewer.ready');page.evaluate('dagViewer.pending')
  check_layouts(page,url)
  # Uneven servers with independent source branches and nested process frames.
  # This used to overlap after equalization and flattening, despite simple DAGs
  # passing. Keep the fixture synthetic and independent of a user's Job history.
  page.evaluate("""async () => {
   const source=(id,node)=>({id,node,node_identity:node,operator:'item_source@1.0',state:'succeeded',inputs:[]});
   const transform=(id,node,inputs)=>({...source(id,node),operator:'transform@1.0',inputs:inputs.map((task,i)=>({task,output_port:'result',port:'input'+i}))});
   const steps=[...Array.from({length:6},(_,i)=>source('source'+i,'server A')),source('source6','server B'),transform('transform0','server A',['source0','source1']),transform('transform1','server A',['source4','source5']),transform('transform2','server B',['source2','source3']),transform('transform3','server B',['transform0','transform2']),transform('transform4','server B',['source6','transform1','transform3'])];
   const nodes=Object.fromEntries(['server A','server B'].map(id=>[id,{execution_units:steps.filter(s=>s.node===id).map(s=>({id:s.id,steps:[s.id],mode:s.inputs.length?'isolated_step':'host_stream_source',runtime_name:'test-runtime',runtime_version:'1.0'}))}]));
   await dagViewer.update({job_id:'nested-group-regression',state:'succeeded',steps,nodes,outputs:{result:{task:'transform4',port:'result'}}},new Map(),dagViewer.lastRequest[2]);
  }""")
  check_layouts(page,'nested-group regression')
  # Exercise a fan-in taller than the usual card, with non-default output ports.
  page.evaluate("""async () => {
   const sources=Array.from({length:8},(_,i)=>({id:'source'+i,node:'fixture',operator:'source@1',state:'succeeded',inputs:[]}));
   const join={id:'join',node:'fixture',operator:'join@1',state:'succeeded',inputs:sources.map((s,i)=>({task:s.id,output_port:'chunk_'+i,port:'input_'+i}))};
   const job={job_id:'port-regression',state:'succeeded',steps:[...sources,join],nodes:{},outputs:{result:{task:'join',port:'merged'}}};
   await dagViewer.update(job,new Map(),dagViewer.lastRequest[2]);
  }""")
  for direction in ['horizontal','vertical']:
   page.locator('[data-dag-layout=swimlane]').click();page.locator(f'[data-dag-direction={direction}]').click();page.evaluate('dagViewer.pending')
   assert page.locator('#dag-error').is_hidden(),page.locator('#dag-error').inner_text()
   boxes=page.evaluate('dagViewer.graph.getNodeData().map(n=>dagViewer.graph.getElementRenderBounds(n.id))')
   for i,a in enumerate(boxes):
    for other in boxes[i+1:]:
     assert a['max'][0]<=other['min'][0] or other['max'][0]<=a['min'][0] or a['max'][1]<=other['min'][1] or other['max'][1]<=a['min'][1],(a,other)
  page.locator('[data-dag-layout=dependency]').click();page.evaluate('dagViewer.pending')
  page.evaluate('async () => {const [job,previous,fmt]=dagViewer.lastRequest;await dagViewer.update({...job,job_id:"logical-port-regression",logical:true},previous,fmt);}')
  assert page.evaluate('dagViewer.graph.getComboData().length')==0
  assert page.evaluate('dagViewer.graph.getNodeData().find(n=>n.id==="task:join").style.size[1]')==246
  print(url,'named ports, tall fan-in, logical plan OK',flush=True)
  assert not errors,errors
  page.close()
 b.close()

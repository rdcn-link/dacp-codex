/* DACP execution viewer. G6 owns rendering, layout, grouping and viewport interaction. */
'use strict';
window.DacpDag=(()=>{
 const palette=[{stroke:'#2563eb',fill:'#f1f5fe'},{stroke:'#c2410c',fill:'#fff7f0'},{stroke:'#15803d',fill:'#f0faf4'},{stroke:'#9333ea',fill:'#f8f3fd'},{stroke:'#be123c',fill:'#fff3f6'},{stroke:'#0e7490',fill:'#f0fafc'}];
 const states={skipped:{color:'#94a3b8',fill:'#f1f5f9',label:'↷ Skipped'},replaying:{color:'#7c3aed',fill:'#f5f3ff',label:'↻ Replaying'},succeeded:{color:'#168264',fill:'#f0faf5',label:'✓ Completed'},running:{color:'#2563eb',fill:'#eff6ff',label:'▶ Running'},failed:{color:'#cc4560',fill:'#fff3f5',label:'! Execution failed'},cancelled:{color:'#cc4560',fill:'#fff3f5',label:'! Cancelled'},expired:{color:'#cc4560',fill:'#fff3f5',label:'! Timed out'},interrupted:{color:'#cc4560',fill:'#fff3f5',label:'! Service interrupted'},unknown:{color:'#b68424',fill:'#fff9eb',label:'? Unknown status'}};
 const pending={color:'#94a3b8',fill:'#f8fafc',label:'○ Not started',dash:[5,4]};
 const compact=(text,max)=>Array.from(String(text||'')).length>max?Array.from(String(text)).slice(0,max-1).join('')+'…':String(text||'');
 function buildData(job,previous,fmt){
  const nodes=[],edges=[],combos=[],details=new Map(),steps=new Map(job.steps.map(t=>[t.id,t])),groups=new Map();
  for(const t of job.steps)if(!groups.has(t.node)){
   const index=groups.size,id='server:'+index,color=palette[index%palette.length];groups.set(t.node,id);
   const count=job.steps.filter(other=>other.node===t.node).length;
   combos.push({id,data:{kind:'server',title:t.node,subtitle:count+' steps',...color},style:{labelText:t.node+'  ·  '+count+' steps',stroke:color.stroke,fill:color.fill}});
   details.set(id,{title:'Execution server',lines:[t.node,count+' steps','Double-click group backgrounds to collapse / expand']});
  }
  function metric(task,direction,port){return job.nodes[task?.node_identity]?.ports?.[task?.id+'/'+direction+'/'+port];}
  function aggregate(task,direction){const all=Object.entries(job.nodes[task.node_identity]?.ports||{}).filter(([key])=>key.startsWith(task.id+'/'+direction+'/')).map(([,m])=>m);return all.length?fmt.bytes(all.reduce((sum,m)=>sum+(m.payload_bytes||0),0)):'—';}
  function flow(task,direction,port){
   const m=metric(task,direction,port);if(!m)return {label:'No observation',lines:['No traffic observed'],remote:false};
   const observed=job.nodes[task.node_identity]?.observed_at_ms||0,last=previous.get(job.job_id+'/'+task.node_identity+'/'+task.id+'/'+direction+'/'+port),dt=observed-(last?.at||0);
   const rate=!fmt.terminal.has(job.state)&&last&&dt>0&&m.payload_bytes>=last.value?fmt.bytes((m.payload_bytes-last.value)*1000/dt)+'/s':'—';
   return {label:fmt.bytes(m.payload_bytes),remote:m.remote_payload_bytes>0,lines:['Payload '+fmt.bytes(m.payload_bytes),'Rate '+rate,'Cross-node payload '+fmt.bytes(m.remote_payload_bytes),...(m.records===undefined?[]:['Records '+m.records.toLocaleString()]),'Last activity '+fmt.time(m.last_activity_ms)]};
  }
  function edge(source,target,label,task,direction,port,sourcePort='result'){
   const traffic=flow(task,direction,port),id='flow:'+target+':'+port+':'+source;
   edges.push({id,source,target,data:{port:label,traffic:traffic.label},style:{sourcePort:source.startsWith('task:')?'output:'+sourcePort:'out',targetPort:target.startsWith('task:')?'input:'+port:'in',labelText:label+'\n'+traffic.label,stroke:traffic.remote?'#5c7bb5':'#a3b2c5',lineWidth:traffic.remote?1.8:1.3}});
   const estimate=(job.planning?.edges||[]).find(e=>'task:'+e.from===source&&'task:'+e.to===target);
   details.set(id,{title:'Data flow · '+label,lines:[...traffic.lines,...(estimate?['Planned transport: '+estimate.mode,'Estimated payload: '+fmt.bytes(estimate.estimated_bytes),'Estimated transfer time: '+fmt.duration(estimate.estimated_transfer_ms)]:[])]});
  }
  for(const t of job.steps){
   const state=states[t.state]||{...pending,label:pending.label+' · '+(fmt.labels[t.state]||t.state||'Waiting')};
   const incoming=aggregate(t,'inputs'),outgoing=aggregate(t,'outputs'),observed=job.nodes[t.node_identity]?.observed_at_ms;
   const inputs=[...new Set((t.inputs||[]).map(b=>b.port))],outputs=[...new Set([...job.steps.flatMap(other=>(other.inputs||[]).filter(b=>b.task===t.id).map(b=>b.output_port||'result')),...Object.values(job.outputs).filter(b=>b.task===t.id).map(b=>b.port||'result')])];
   const height=(job.logical?70:118)+Math.max(1,inputs.length,outputs.length)*22;
   nodes.push({id:'task:'+t.id,combo:groups.get(t.node),data:{kind:'task',title:t.title||t.id,subtitle:t.operator,stateLabel:state.label,border:t.state==='running'?1.8:1,color:state.color,fill:'#fff',statusFill:state.fill,dash:[],incoming,outgoing,inputs,outputs},style:{size:[252,height]}});
   details.set('task:'+t.id,{title:t.title||t.id,lines:['Step ID: '+t.id,'Operator: '+t.operator,'Server: '+t.node,'Status: '+(fmt.labels[t.state]||t.state),...(t.replayed?['Replaying from materialized results']:[]),'Input payload: '+incoming,'Output payload: '+outgoing,'Last observed: '+fmt.time(observed)]});
  }
  for(const t of job.steps)for(const binding of t.inputs){
   let source='task:'+binding.task;
   if(!binding.task){
    source='input:'+t.id+':'+binding.port;
    let combo=groups.get(t.node),server=t.node;
    if(binding.remote){
     server=binding.upstream_origin||binding.uri?.split(' / ')[0]||'Upstream server';
     combo=groups.get(server);
     if(!combo){
      const color=palette[groups.size%palette.length];combo='upstream:'+groups.size;groups.set(server,combo);
      combos.push({id:combo,data:{kind:'server',title:server,subtitle:'External output interfaces',...color},style:{labelText:server+' · External outputs',stroke:color.stroke,fill:color.fill}});
      details.set(combo,{title:'Upstream server',lines:[server,'Only output interfaces are shown; upstream steps execute on this server.']});
     }
    }
    const title=binding.upstream_task?binding.upstream_task+'.'+(binding.upstream_port||'result'):binding.uri?.split('/').pop()||binding.port;
    nodes.push({id:source,combo,data:{kind:'input',title,subtitle:(binding.remote?'External output':'Local input')+' · '+binding.port,color:'#7f93ad',fill:'#fff',dash:[]},style:{size:[216,76]}});
    details.set(source,{title:(binding.remote?'Cross-node input':'Local input')+' · '+binding.port,lines:[binding.uri||'', 'Meaning: '+(binding.view||'bytes'),'Server: '+server]});
   }
   edge(source,'task:'+t.id,binding.task?(binding.output_port||'result')+' → '+binding.port:binding.port,t,'inputs',binding.port,binding.output_port||'result');
  }
  if(Object.keys(job.outputs).length){
   combos.push({id:'client',data:{title:'Result outlet',subtitle:job.output_route||'Union → Client'},style:{labelText:'Result outlet · '+(job.output_route||'Union → Client'),stroke:'#a3b2c5',fill:'#f5f7fa'}});
   details.set('client',{title:'Streaming result outlet',lines:[job.output_route||'Results forwarded through Union to the submitting client','Only execution metadata is shown; result streams are not consumed.']});
  }
  for(const [label,binding]of Object.entries(job.outputs)){
   const id='output:'+label;
  nodes.push({id,combo:'client',data:{kind:'output',title:label,subtitle:job.output_route||'Union → Client',color:'#6f8ba7',fill:'#fff',dash:[]},style:{size:[216,76]}});
   details.set(id,{title:'Output · '+label,lines:[binding.task+'.'+binding.port,job.output_route||'Forwarded through Union to the submitting client']});
   edge('task:'+binding.task,id,binding.port,steps.get(binding.task),'outputs',binding.port,binding.port||'result');
  }
  if(!job.logical){
   const modeNames={fusion_candidate:'Fusion candidate (estimated)',isolated_step_candidate:'Separate process (estimated)',shared_process:'Shared process',isolated_step:'Separate execution',host_stream_source:'Host stream source',remote_subjob:'Remote sub-job'};
   const executionNodes={...(job.nodes||{})};
   for(const planned of job.planning?.execution_units||[]){
    const task=job.steps.find(t=>t.node===planned.node);if(!task)continue;
    const identity=task.node_identity;if(!identity||job.nodes?.[identity]?.execution_units?.length)continue;
    if(executionNodes[identity]===job.nodes?.[identity])executionNodes[identity]={...executionNodes[identity],execution_units:[]};
    if(!executionNodes[identity])executionNodes[identity]={execution_units:[]};
    executionNodes[identity].execution_units.push({...planned,mode:planned.mode==='isolated_step'?'isolated_step_candidate':planned.mode,runtime_image:planned.runtime==='unknown'?'':planned.runtime,state:'estimated',reason:'Candidate only; member preparation determines actual execution units'});
   }
   for(const [identity,snapshot] of Object.entries(executionNodes)){
    for(const unit of snapshot.execution_units||[]){
     const members=nodes.filter(n=>n.data.kind==='task'&&steps.get(n.id.slice(5))?.node_identity===identity&&(unit.steps||[]).includes(n.id.slice(5)));
     if(!members.length)continue;
     const id='unit:'+identity+':'+unit.id,parent=members[0].combo;
     const label=modeNames[unit.mode]||unit.mode;
     const environment=unit.dependency_environment?'Env '+unit.dependency_environment.slice(0,10):'';
     const runtime=unit.runtime_name?unit.runtime_name+(unit.runtime_version?'@'+unit.runtime_version:''):unit.mode==='host_stream_source'?'Host JVM':unit.runtime_image?unit.runtime_executor||'Container':'Runtime not recorded';
     combos.push({id,combo:parent,data:{kind:'process',title:label,runtime,subtitle:members.length+' steps'},style:{labelText:label+' · '+members.length+' steps\n'+runtime+(environment?' · '+environment:''),stroke:unit.mode==='shared_process'?'#0d9488':'#94a3b8',fill:unit.mode==='shared_process'?'#f0fdfa':'#f8fafc',lineDash:[5,4],radius:12,padding:[80,24,24,24],labelTextBaseline:'top',labelOffsetX:14,labelOffsetY:12,labelFontSize:11,labelLineHeight:16,labelBackground:true,labelBackgroundFill:unit.mode==='shared_process'?'#f0fdfa':'#f8fafc',labelPadding:[3,5]}});
     details.set(id,{title:'Execution unit · '+label,lines:['Server: '+steps.get(members[0].id.slice(5)).node,'Steps: '+unit.steps.join(', '),'Runtime: '+runtime,'Image: '+(unit.runtime_image||'Not applicable / not recorded'),'Dependency environment: '+(unit.dependency_environment||'Runtime only'),'Processes: '+(unit.process_count||1),...Object.entries(unit.runtime_dependencies||{}).map(([name,version])=>name+': '+version),'Status: '+(unit.state||snapshot.state||job.state||'Unknown'),...(unit.reason?['Planning reason: '+unit.reason]:[])]});
     for(const member of members){member.combo=id;const d=details.get(member.id);d.lines.push('Execution unit: '+label,...(unit.runtime_image?['Runtime: '+unit.runtime_image]:[]));}
    }
   }
   for(const task of job.steps){
    const decision=(job.placement_decisions||[]).find(d=>d.step===task.id),detail=details.get('task:'+task.id);
    const estimate=(job.planning?.steps||[]).find(s=>s.step===task.id);if(estimate&&detail)detail.lines.push('Estimated output: '+fmt.bytes(estimate.output_bytes),'Estimated compute: '+fmt.duration(estimate.compute_ms),'Estimate basis: '+estimate.confidence);
    if(decision&&detail)detail.lines.push('Placement reason: '+decision.reason,...(decision.requested_source?['Source: '+decision.requested_source+' → '+decision.resolved_source]:[]));
    const node=nodes.find(n=>n.id==='task:'+task.id);
    if(node&&node.combo===groups.get(task.node))detail.lines.push('Execution unit: not recorded / pending planning');
   }
  }
  if(job.logical){
   combos.length=0;
   for(const node of nodes){delete node.combo;node.data.logical=true;node.data.color='#5679a6';node.data.fill='#fff';node.data.statusFill='#edf3ff';node.data.dash=[];
    if(node.data.kind==='task'){const task=steps.get(node.id.slice(5));
     node.data.stateLabel='Logical step';
     details.set(node.id,{title:task.title||task.id,lines:['Step: '+task.id,'Operator: '+task.operator,...Object.entries(task.params||{}).map(([key,value])=>key+': '+String(value))]});}
   }
   for(const edge of edges){edge.style.labelText=edge.data.port;edge.style.stroke='#94a7bd';details.set(edge.id,{title:'Stream dependency',lines:[edge.data.port]});}
   for(const id of [...details.keys()])if(id.startsWith('server:')||id==='client')details.delete(id);
  }
  const topology=JSON.stringify({nodes:nodes.map(n=>[n.id,n.combo]),edges:edges.map(e=>[e.id,e.source,e.target]),combos:combos.map(c=>c.id)});
  return {data:{nodes,edges,combos},details,topology};
 }
 // Equalize the cross-flow dimension within each physical region type.
 function equalizeRegions(data,bounds,vertical,kind){
  const byId=new Map(data.combos.map(c=>[c.id,c]));
  const visible=c=>{for(let current=c;current;current=byId.get(current.combo))if(current.style?.collapsed)return false;return true;};
  const groups=data.combos.filter(c=>c.data?.kind===kind&&visible(c));
  if(!groups.length)return [];
  const span=c=>{const b=bounds(c.id);return vertical?b.right-b.left:b.bottom-b.top;};
  const target=Math.ceil((Math.max(...groups.map(span))-.01)/16)*16,changed=[];
  for(const c of groups){const delta=target-span(c);if(delta<.5)continue;
   const padding=[...(c.style?.padding||[64,24,24,24])];
   // Keep headers anchored and add breathing room equally at either side.
   if(vertical){padding[1]+=delta/2;padding[3]+=delta/2;}else{padding[0]+=delta/2;padding[2]+=delta/2;}
   c.style={...c.style,padding};changed.push({id:c.id,style:{padding}});
  }
  return changed;
 }
 // Swimlanes share global dependency columns; each server occupies a distinct row.
 function swimlane(data,vertical){
  const ranks=new Map(data.nodes.map(n=>[n.id,0])),remaining=new Map(data.nodes.map(n=>[n.id,0])),next=new Map(data.nodes.map(n=>[n.id,[]]));
  for(const e of data.edges){remaining.set(e.target,remaining.get(e.target)+1);next.get(e.source).push(e.target);}
  const queue=data.nodes.filter(n=>remaining.get(n.id)===0).map(n=>n.id);
  for(let i=0;i<queue.length;i++)for(const target of next.get(queue[i])){ranks.set(target,Math.max(ranks.get(target),ranks.get(queue[i])+1));remaining.set(target,remaining.get(target)-1);if(!remaining.get(target))queue.push(target);}
  const combos=new Map(data.combos.map(c=>[c.id,c])),byId=new Map(data.nodes.map(n=>[n.id,n]));
  function rootOf(node){let id=node.combo;while(combos.get(id)?.combo)id=combos.get(id).combo;return id;}
  function union(boxes){return {left:Math.min(...boxes.map(b=>b.left)),top:Math.min(...boxes.map(b=>b.top)),right:Math.max(...boxes.map(b=>b.right)),bottom:Math.max(...boxes.map(b=>b.bottom))};}
  function content(id){return union([...data.nodes.filter(n=>n.combo===id).map(n=>bounds(n.id)),...data.combos.filter(c=>c.combo===id).map(c=>bounds(c.id))]);}
  function bounds(id){
   const n=byId.get(id);
   if(n)return {left:n.style.x-n.style.size[0]/2,top:n.style.y-n.style.size[1]/2,right:n.style.x+n.style.size[0]/2,bottom:n.style.y+n.style.size[1]/2};
   const c=combos.get(id),box=content(id),pad=c.style.padding||[64,24,24,24];
   return {left:box.left-pad[3],top:box.top-pad[0],right:box.right+pad[1],bottom:box.bottom+pad[2]};
  }
  const roots=data.combos.filter(c=>!c.combo),lanes=[],rankStride=Math.max(...data.nodes.map(n=>n.style.size[1]))+180;
  for(const combo of roots){
   const members=data.nodes.filter(n=>rootOf(n)===combo.id);if(!members.length)continue;
   // Include nested execution-unit headers and padding in the slot size.
   const stride=Math.max(...members.map(n=>{
    let span=n.style.size[vertical?0:1],parent=n.combo;
    while(parent&&parent!==combo.id){const c=combos.get(parent),p=c.style.padding||[64,24,24,24];span+=vertical?p[1]+p[3]:p[0]+p[2];parent=c.combo;}
    return span;
   }))+48;
   const columns=new Map();
   for(const n of members){const rank=ranks.get(n.id),slot=columns.get(rank)||0;columns.set(rank,slot+1);n.style={...n.style,x:vertical?slot*stride:rank*400,y:vertical?rank*rankStride:slot*stride};}
   lanes.push({combo,members,box:content(combo.id)});
  }
  if(!lanes.length)return;
  equalizeRegions(data,bounds,vertical,'process');
  for(const lane of lanes)lane.box=content(lane.combo.id);
  const start=Math.min(...lanes.map(l=>vertical?l.box.top:l.box.left)),end=Math.max(...lanes.map(l=>vertical?l.box.bottom:l.box.right));
  for(const {combo,box} of lanes)combo.style.padding=vertical?[box.top-start+64,30,end-box.bottom+30,30]:[64,end-box.right+30,30,box.left-start+30];
  equalizeRegions(data,bounds,vertical,'server');
  let cursor=0;
  for(const {combo,members,box} of lanes){
   // Align lane lengths using child-group bounds, then pack their full outer
   // bounds on the cross axis. Fixed card-height increments miss nested frames.
   const outer=bounds(combo.id),offset=cursor-(vertical?outer.left:outer.top);
   for(const n of members)n.style[vertical?'x':'y']+=offset;
   cursor+=(vertical?outer.right-outer.left:outer.bottom-outer.top)+80;
  }
 }
 function registerCard(){
  class TaskCard extends G6.Rect {
   render(attributes=this.parsedAttributes,container){
    super.render(attributes,container);
    const d=attributes.card||{},task=d.kind==='task',size=attributes.size||[252,140],width=size[0],height=size[1],left=-width/2,top=-height/2;
    const text=(key,value,x,y,size,fill,weight=400,align='left')=>this.upsert(key,'text',{x,y,text:value,fontFamily:'system-ui, sans-serif',fontSize:size,fill,fontWeight:weight,textBaseline:'middle',textAlign:align,pointerEvents:'none'},container);
    this.upsert('accent','rect',{x:left+1,y:top+1,width:width-2,height:3,radius:[9,9,0,0],fill:d.color,pointerEvents:'none'},container);
    const symbol=task?'M4 6h16M4 12h16M4 18h16M8 3v6M16 9v6M9 15v6':d.kind==='output'?'M5 5h14v14H5zM9 12h10M15 8l4 4-4 4':'M6 3h8l4 4v14H6zM14 3v5h5M9 12h6M9 16h6';
    const svg='<svg xmlns="http://www.w3.org/2000/svg" width="24" height="24" viewBox="0 0 24 24"><path d="'+symbol+'" fill="none" stroke="'+d.color+'" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"/></svg>';
    this.upsert('operator-icon','image',{x:width/2-38,y:top+15,width:23,height:23,src:'data:image/svg+xml;charset=utf-8,'+encodeURIComponent(svg),pointerEvents:'none'},container);
    text('title',compact(d.title,task?23:21),left+14,top+25,12,'#21324c',600);
    text('subtitle',compact(d.subtitle,task?35:29),left+14,top+46,10,'#77869b');
    if(task){
     const portY=d.logical?80:104;
     if(!d.logical){
     this.upsert('status-background','rect',{x:left+13,y:top+61,width:Math.min(width-26,20+Array.from(d.stateLabel||'').length*5.6),height:19,radius:5,fill:d.statusFill||'#f2f5fa',pointerEvents:'none'},container);
     text('status',d.stateLabel,left+20,top+71,10,d.color,500);
     }
     this.upsert('divider','line',{x1:left+1,x2:width/2-1,y1:top+portY-15,y2:top+portY-15,stroke:'#edf1f6',lineWidth:1,pointerEvents:'none'},container);
     // Named ports mirror the designer, while the inspector retains full traffic details.
     for(const [side,ports]of [['input',d.inputs||[]],['output',d.outputs||[]]]){
      for(let i=0;i<ports.length;i++)text(side+'-label-'+i,compact(ports[i],18),side==='input'?left+14:width/2-14,top+portY+i*22,10,'#5c779d',400,side==='input'?'left':'right');
     }
     if(!d.logical){
      this.upsert('traffic-divider','line',{x1:left+14,x2:width/2-14,y1:top+height-25,y2:top+height-25,stroke:'#edf1f6',lineWidth:1,pointerEvents:'none'},container);
      text('traffic','↓ '+d.incoming+'    ↑ '+d.outgoing,left+14,top+height-12,9,'#8291a5');
     }
    }
   }
  }
  class ExecutionRegion extends G6.RectCombo {
   render(attributes=this.parsedAttributes,container){
    super.render(attributes,container);
    const d=attributes.region||{},process=d.kind==='process',key=this.getKeyStyle(attributes),{x,y,width,height}=key;
    const header=Math.min(process?60:52,height-8),color=process?'#7890a5':d.stroke||'#55779b';
    this.upsert('region-header','rect',{x:x+2,y:y+2,width:Math.max(0,width-4),height:header,radius:[12,12,0,0],fill:'#fff',fillOpacity:process?.55:.35,pointerEvents:'none'},container);
    this.upsert('header-divider','line',{x1:x+12,x2:x+width-12,y1:y+header,y2:y+header,stroke:color,strokeOpacity:.18,lineWidth:1,pointerEvents:'none'},container);
    const text=(id,value,tx,ty,size,weight=400)=>this.upsert(id,'text',{x:tx,y:ty,text:value,fontFamily:'system-ui, sans-serif',fontSize:size,fontWeight:weight,textBaseline:'top',fill:'#36536d',pointerEvents:'none'},container);
    if(process){
     this.upsert('process-icon','rect',{x:x+14,y:y+15,width:22,height:22,radius:4,fill:'#fff',stroke:color,lineWidth:1.3,pointerEvents:'none'},container);
     this.upsert('process-icon-bar','line',{x1:x+14,x2:x+36,y1:y+21,y2:y+21,stroke:color,lineWidth:1,pointerEvents:'none'},container);
     text('process-icon-prompt','›_',x+19,y+23,10,600);
     text('region-title',compact(d.title+' · '+d.subtitle,Math.max(12,Math.floor((width-72)/6))),x+46,y+13,11,600);
     text('region-subtitle',compact(d.runtime,Math.max(12,Math.floor((width-72)/6))),x+46,y+31,10);
     for(let i=0;i<3;i++)this.upsert('window-control-'+i,'circle',{cx:x+width-30+i*7,cy:y+9,r:1.6,fill:color,fillOpacity:.5,pointerEvents:'none'},container);
    }else{
     for(let i=0;i<3;i++){
      this.upsert('rack-'+i,'rect',{x:x+15,y:y+12+i*9,width:25,height:7,radius:2,fill:'#fff',stroke:color,lineWidth:1.1,pointerEvents:'none'},container);
      this.upsert('rack-light-'+i,'circle',{cx:x+20,cy:y+15.5+i*9,r:1.3,fill:['#20aa87','#e9a438','#5797db'][i],pointerEvents:'none'},container);
      this.upsert('rack-slot-'+i,'line',{x1:x+26,x2:x+35,y1:y+15.5+i*9,y2:y+15.5+i*9,stroke:color,lineWidth:1,pointerEvents:'none'},container);
     }
     text('region-title',compact(d.title,Math.max(12,Math.floor((width-72)/6.5))),x+52,y+12,12,600);
     text('region-subtitle','Node server · '+d.subtitle,x+52,y+32,10);
    }
   }
  }
  G6.register(G6.ExtensionCategory.COMBO,'dacp-execution-region',ExecutionRegion);
  G6.register(G6.ExtensionCategory.NODE,'dacp-task-card',TaskCard);
 }
 let registered=false;
 class Viewer {
  constructor(host){
   this.host=host;this.graph=null;this.ready=false;this.job=null;this.topology=null;this.details=new Map();this.inspected=null;this.pending=Promise.resolve();this.mode='fit';this.layoutMode='dependency';this.direction='horizontal';this.mouseZoom=false;
   document.querySelectorAll('[data-dag-layout]').forEach(button=>button.addEventListener('click',()=>this.setLayout('layout',button.dataset.dagLayout)));
   document.querySelectorAll('[data-dag-direction]').forEach(button=>button.addEventListener('click',()=>this.setLayout('direction',button.dataset.dagDirection)));
   const button=(id,fn)=>document.getElementById(id).addEventListener('click',()=>this.action(fn));
   // G6 cancels wheel events even when its zoom behavior is disabled.
   // Cancel only while unlocked so native page scrolling works while locked.
   host.addEventListener('wheel',event=>{if(this.mouseZoom)event.preventDefault();},{passive:false});
   const lock=document.getElementById('dag-zoom-lock');
   lock.addEventListener('click',()=>{this.mouseZoom=!this.mouseZoom;host.classList.toggle('mouse-zoom-enabled',this.mouseZoom);lock.textContent=this.mouseZoom?'🔓 Mouse zoom':'🔒 Mouse zoom';lock.setAttribute('aria-pressed',String(this.mouseZoom));lock.title=this.mouseZoom?'Lock mouse zoom':'Unlock mouse zoom';this.zoomHint();});
   button('dag-zoom-in',async()=>{this.mode='manual';await this.graph.zoomTo(Math.min(2.5,this.graph.getZoom()*1.2),false);});
   button('dag-zoom-out',async()=>{this.mode='manual';await this.graph.zoomTo(Math.max(.15,this.graph.getZoom()/1.2),false);});
   button('dag-fit',()=>this.fit());
   button('dag-reset',async()=>{this.mode='manual';await this.graph.zoomTo(1,false);});
   button('dag-expand',async()=>{for(const combo of this.graph.getComboData())if(combo.style?.collapsed)await this.graph.expandElement(combo.id,{animation:false});await this.normalizeRegions();await this.fit();});
   this.resize=new ResizeObserver(()=>{if(this.graph&&this.host.clientWidth&&this.host.clientHeight)this.action(async()=>{this.graph.resize(this.host.clientWidth,this.host.clientHeight);if(this.mode==='fit')await this.fit();});});
   this.resize.observe(host);
  }
  setLayout(control,value){
   const field=control==='layout'?'layoutMode':'direction',values=control==='layout'?['dependency','swimlane']:['horizontal','vertical'];
   if(!values.includes(value)||this[field]===value)return;
   this[field]=value;
   document.querySelectorAll('[data-dag-layout]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.dagLayout===this.layoutMode)));
   document.querySelectorAll('[data-dag-direction]').forEach(button=>button.setAttribute('aria-pressed',String(button.dataset.dagDirection===this.direction)));
   document.getElementById('dag-expand').disabled=this.layoutMode==='swimlane';
   this.zoomHint();
   if(this.lastRequest)this.update(...this.lastRequest).catch(error=>{const box=document.getElementById('dag-error');box.textContent='Graph layout failed: '+error.message;box.hidden=false;});
  }
  zoomHint(){
   document.getElementById('dag-layout-hint').textContent=this.layoutMode==='swimlane'?(this.direction==='vertical'?'Columns by server · top-to-bottom flow':'Rows by server · left-to-right flow')+' · drag to pan':'Drag to pan · double-click groups to collapse';
   document.getElementById('dag-layout-hint').textContent+=this.mouseZoom?' · scroll to zoom':' · mouse zoom locked';
  }
  action(fn){
   this.pending=this.pending.catch(()=>{}).then(async()=>{if(this.graph){await fn();this.zoomLabel();}}).catch(error=>{const box=document.getElementById('dag-error');box.textContent='Graph interaction failed: '+error.message;box.hidden=false;});return this.pending;
  }
  zoomLabel(){if(!this.graph||!this.ready)return;const zoom=this.graph.getZoom();document.getElementById('dag-zoom-label').textContent=Math.round(zoom*100)+'%';document.getElementById('dag-zoom-out').disabled=zoom<=.15;document.getElementById('dag-zoom-in').disabled=zoom>=2.5;}
  async fit(){this.mode='fit';await this.graph.fitView({when:'always',direction:'both'},false);this.zoomLabel();}
  select(id){
   const previous=this.inspected;this.inspect(id);
   return this.action(async()=>{
    const states={};
    if(previous&&this.details.has(previous))states[previous]=[];
    if(id&&this.details.has(id))states[id]=['selected'];
    if(Object.keys(states).length)await this.graph.setElementState(states,false);
   });
  }
  inspect(id){
   this.inspected=id;const info=this.details.get(id),box=document.getElementById('dag-inspect');box.replaceChildren();
   if(!info){box.textContent='Click a step, input or edge for full details.';return;}
   const title=document.createElement('strong');title.textContent=info.title;box.append(title);
   for(const line of info.lines){const row=document.createElement('span');row.textContent=line;box.append(row);}
  }
  update(job,previous,fmt){
   this.lastRequest=[job,previous,fmt];
   const layout=this.layoutMode,vertical=this.direction==='vertical',layoutKey=layout+':'+this.direction,model=buildData(job,previous,fmt);
   if(layout==='swimlane')swimlane(model.data,vertical);
   // Serialize viewport operations and refreshes; a slow render must finish before the next update.
   const work=this.pending.catch(()=>{}).then(async()=>{
    if(!window.G6)throw new Error('G6 Graph component failed to load');
    if(!registered){registerCard();registered=true;}
    this.details=model.details;
    const rebuild=!this.ready||this.job!==job.job_id||this.topology!==model.topology||this.renderedLayout!==layoutKey;
    if(rebuild){
     const selected=this.job===job.job_id&&model.details.has(this.inspected)?this.inspected:null;
     this.destroyGraph();this.renderedLayout=layoutKey;document.getElementById('dag-error').hidden=true;this.host.replaceChildren();this.job=job.job_id;this.topology=model.topology;this.mode='fit';this.inspect(selected);
     this.graph=new G6.Graph({
      container:this.host,width:this.host.clientWidth,height:this.host.clientHeight,animation:false,padding:36,zoomRange:[.15,2.5],data:model.data,
      layout:layout==='swimlane'?undefined:{type:'combo-combined',comboPadding:[64,24,24,24],comboSpacing:64,layout:comboId=>({type:'dagre',rankdir:vertical?'TB':'LR',nodesep:comboId?36:70,ranksep:comboId?80:100})},
      node:{type:'dacp-task-card',state:{selected:{stroke:'#346ded',lineWidth:2,shadowColor:'#346ded30',shadowBlur:14,shadowOffsetY:2}},style:{card:d=>d.data,fill:d=>d.data.fill,stroke:'#cbd9ed',lineDash:d=>d.data.dash,lineWidth:d=>d.data.border||1,radius:10,shadowColor:'#24436618',shadowBlur:14,shadowOffsetY:5,label:false,ports:d=>{
       if(d.data.kind!=='task')return [{key:'in',placement:vertical?'top':'left'},{key:'out',placement:vertical?'bottom':'right'}];
       const size=d.style.size;
       return ['input','output'].flatMap(side=>(d.data[side==='input'?'inputs':'outputs']||[]).map((port,i)=>({key:side+':'+port,placement:vertical?[(i+1)/((d.data[side==='input'?'inputs':'outputs']||[]).length+1),side==='input'?0:1]:[side==='input'?0:1,((d.data.logical?80:104)+i*22)/size[1]]})));
      },portR:3.5,portFill:'#fff',portStroke:'#7192c3',portLineWidth:1.5}},
      edge:{type:vertical?'cubic-vertical':'cubic-horizontal',state:{selected:{stroke:'#2563eb',lineWidth:4,shadowColor:'#2563eb55',shadowBlur:10,labelFill:'#1d4ed8',labelFontWeight:700,labelBackgroundFill:'#dbeafe'}},style:{sourcePort:d=>d.style?.sourcePort||'out',targetPort:d=>d.style?.targetPort||'in',endArrow:true,endArrowSize:6,labelFontFamily:'system-ui, sans-serif',labelFontSize:10,labelFill:'#5b718c',labelBackground:true,labelBackgroundFill:'#ffffffed',labelBackgroundRadius:5,labelPadding:[2,5],labelLineHeight:15,labelAutoRotate:false,labelPlacement:'center'}},
      combo:{type:d=>d.data?.kind?'dacp-execution-region':'rect',state:{selected:{stroke:'#2563eb',lineWidth:3.5,shadowColor:'#2563eb44',shadowBlur:16}},style:{region:d=>d.data,label:d=>!d.data?.kind,radius:18,padding:d=>d.style?.padding||[64,24,24,24],lineWidth:1.2,fillOpacity:1,labelPlacement:'top-left',labelTextAlign:'left',labelTextBaseline:'top',labelOffsetX:16,labelOffsetY:14,labelFontSize:12,labelFontWeight:600,labelFill:'#526b88',collapsedSize:[252,76]}},
      behaviors:[{type:'drag-canvas',onFinish:()=>{this.mode='manual';}},{type:'zoom-canvas',enable:()=>this.mouseZoom,preventDefault:false,sensitivity:.5,onFinish:()=>{this.mode='manual';this.zoomLabel();}},{type:'collapse-expand',enable:event=>layout!=='swimlane'&&event.targetType==='combo',animation:false,onCollapse:()=>this.action(()=>this.normalizeRegions()),onExpand:()=>this.action(()=>this.normalizeRegions())}],
     });
     for(const type of ['node','edge','combo'])this.graph.on(type+':click',event=>this.select(event.target.id));
     this.graph.on('canvas:click',()=>this.select(null));
     this.graph.on('aftertransform',()=>this.zoomLabel());
     await this.graph.render();
     if(layout!=='swimlane')await this.normalizeRegions();
     if(selected)await this.graph.setElementState({[selected]:['selected']},false);this.ready=true;await this.fit();
    }else{
     // Retain G6-computed coordinates, edge routing, collapsed groups and viewport.
     this.graph.updateData({nodes:model.data.nodes.map(({style,...node})=>node),edges:model.data.edges.map(e=>({id:e.id,data:e.data,style:{labelText:e.style.labelText,stroke:e.style.stroke,lineWidth:e.style.lineWidth,lineDash:e.style.lineDash}}))});
     await this.graph.draw();if(this.inspected)this.inspect(this.inspected);
    }
    this.zoomLabel();
   });
   this.pending=work;return work;
  }
  async alignRegions(kind){
   const data=this.graph.getData(),vertical=this.direction==='vertical',groups=new Map();
   const byId=new Map(data.combos.map(c=>[c.id,c]));
   const visible=c=>{for(let p=c;p;p=byId.get(p.combo))if(p.style?.collapsed)return false;return true;};
   for(const c of data.combos){
    if(!visible(c))continue;
    if(kind==='process'?c.data?.kind!=='process':!!c.combo)continue;
    const key=c.combo||'',group=groups.get(key)||[];group.push(c);groups.set(key,group);
   }
   for(const [parent,combos] of groups){
    const blocks=[...combos,...(kind==='process'?data.nodes.filter(n=>n.combo===parent):[])].map(c=>{const b=this.graph.getElementRenderBounds(c.id);return {id:c.id,start:b.min[vertical?1:0],end:b.max[vertical?1:0],cross:b.min[vertical?0:1]};});
    if(blocks.length<2)continue;
    blocks.sort((a,b)=>a.start-b.start||a.cross-b.cross||a.id.localeCompare(b.id));
    const baseline=Math.min(...blocks.map(b=>b.cross));let cursor=Math.min(...blocks.map(b=>b.start));
    const translations={};
    for(const b of blocks){const along=Math.max(cursor,b.start)-b.start,across=baseline-b.cross;if(Math.abs(along)>.5||Math.abs(across)>.5)translations[b.id]=vertical?[across,along]:[along,across];cursor=b.end+along+(kind==='process'?70:90);}
    if(Object.keys(translations).length)await this.graph.translateElementBy(translations,false);
   }
  }
  async normalizeRegions(){
   if(!this.graph||this.layoutMode==='swimlane')return;
   const vertical=this.direction==='vertical';
   // Inner frames affect outer frames; remeasure after each layout settles.
   for(let pass=0;pass<3;pass++){
    let changed=false;
    for(const kind of ['process','server']){
     const data=this.graph.getData(),bounds=id=>{const b=this.graph.getElementRenderBounds(id);return {left:b.min[0],top:b.min[1],right:b.max[0],bottom:b.max[1]};};
     const changes=equalizeRegions(data,bounds,vertical,kind);
     if(changes.length){changed=true;this.graph.updateComboData(changes);await this.graph.draw();await this.graph.layout();}
    }
    if(!changed)break;
   }
   await this.alignRegions('process');
   const data=this.graph.getData(),bounds=id=>{const b=this.graph.getElementRenderBounds(id);return {left:b.min[0],top:b.min[1],right:b.max[0],bottom:b.max[1]};};
   const outer=equalizeRegions(data,bounds,vertical,'server');
   if(outer.length){this.graph.updateComboData(outer);await this.graph.draw();}
   await this.alignRegions('server');
  }
  destroyGraph(){this.ready=false;if(this.graph){this.graph.destroy();this.graph=null;}}
  destroy(){this.resize.disconnect();this.destroyGraph();}
 }
 return {Viewer,buildData,equalizeRegions};
})();

/* DACP execution viewer. G6 owns rendering, layout, grouping and viewport interaction. */
'use strict';
window.DacpDag=(()=>{
 const palette=[{stroke:'#2563eb',fill:'#dbeafe'},{stroke:'#c2410c',fill:'#ffedd5'},{stroke:'#15803d',fill:'#dcfce7'},{stroke:'#9333ea',fill:'#f3e8ff'},{stroke:'#be123c',fill:'#ffe4e6'},{stroke:'#0e7490',fill:'#cffafe'}];
 const states={skipped:{color:'#94a3b8',fill:'#f1f5f9',label:'↷ Skipped'},replaying:{color:'#7c3aed',fill:'#f5f3ff',label:'↻ Replaying'},succeeded:{color:'#168264',fill:'#f0faf5',label:'✓ Completed'},running:{color:'#2563eb',fill:'#eff6ff',label:'▶ Running'},failed:{color:'#cc4560',fill:'#fff3f5',label:'! Execution failed'},cancelled:{color:'#cc4560',fill:'#fff3f5',label:'! Cancelled'},expired:{color:'#cc4560',fill:'#fff3f5',label:'! Timed out'},interrupted:{color:'#cc4560',fill:'#fff3f5',label:'! Service interrupted'},unknown:{color:'#b68424',fill:'#fff9eb',label:'? Unknown status'}};
 const pending={color:'#94a3b8',fill:'#f8fafc',label:'○ Not started',dash:[5,4]};
 const compact=(text,max)=>Array.from(String(text||'')).length>max?Array.from(String(text)).slice(0,max-1).join('')+'…':String(text||'');
 function buildData(job,previous,fmt){
  const nodes=[],edges=[],combos=[],details=new Map(),tasks=new Map(job.tasks.map(t=>[t.id,t])),groups=new Map();
  for(const t of job.tasks)if(!groups.has(t.node)){
   const index=groups.size,id='server:'+index,color=palette[index%palette.length];groups.set(t.node,id);
   const count=job.tasks.filter(other=>other.node===t.node).length;
   combos.push({id,data:{title:t.node,subtitle:count+' steps',...color},style:{labelText:t.node+'  ·  '+count+' steps',stroke:color.stroke,fill:color.fill}});
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
  function edge(source,target,label,task,direction,port){
   const traffic=flow(task,direction,port),id='flow:'+target+':'+port+':'+source;
   edges.push({id,source,target,data:{port:label,traffic:traffic.label},style:{targetPort:source.startsWith('task:')&&target.startsWith('task:')?'left':'in',labelText:label+'\n'+traffic.label,stroke:traffic.remote?'#5c7bb5':'#a3b2c5',lineWidth:traffic.remote?1.8:1.3}});
   details.set(id,{title:'Data flow · '+label,lines:traffic.lines});
  }
  for(const t of job.tasks){
   const state=states[t.state]||{...pending,label:pending.label+' · '+(fmt.labels[t.state]||t.state||'Waiting')};
   const incoming=aggregate(t,'inputs'),outgoing=aggregate(t,'outputs'),observed=job.nodes[t.node_identity]?.observed_at_ms;
  nodes.push({id:'task:'+t.id,combo:groups.get(t.node),data:{kind:'task',title:t.title||t.id,subtitle:t.operator,stateLabel:state.label,border:t.state==='running'?2:1.2,color:state.color,fill:state.fill,dash:state.dash||[],incoming,outgoing},style:{size:[268,150]}});
   details.set('task:'+t.id,{title:t.title||t.id,lines:['Task ID: '+t.id,'Operator: '+t.operator,'Server: '+t.node,'Status: '+(fmt.labels[t.state]||t.state),...(t.replayed?['Replaying from materialized results']:[]),'Input payload: '+incoming,'Output payload: '+outgoing,'Last observed: '+fmt.time(observed)]});
  }
  for(const t of job.tasks)for(const binding of t.inputs){
   let source='task:'+binding.task;
   if(!binding.task){
    source='input:'+t.id+':'+binding.port;
    let combo=groups.get(t.node),server=t.node;
    if(binding.remote){
     server=binding.upstream_origin||binding.uri?.split(' / ')[0]||'Upstream server';
     combo=groups.get(server);
     if(!combo){
      const color=palette[groups.size%palette.length];combo='upstream:'+groups.size;groups.set(server,combo);
      combos.push({id:combo,data:{title:server,subtitle:'External output interfaces',...color},style:{labelText:server+' · External outputs',stroke:color.stroke,fill:color.fill}});
      details.set(combo,{title:'Upstream server',lines:[server,'Only output interfaces are shown; upstream steps execute on this server.']});
     }
    }
    const title=binding.upstream_task?binding.upstream_task+'.'+(binding.upstream_port||'result'):binding.uri?.split('/').pop()||binding.port;
    nodes.push({id:source,combo,data:{kind:'input',title,subtitle:(binding.remote?'External output':'Local input')+' · '+binding.port,color:'#7f93ad',fill:'#fff',dash:[]},style:{size:[228,76]}});
    details.set(source,{title:(binding.remote?'Cross-node input':'Local input')+' · '+binding.port,lines:[binding.uri||'', 'Meaning: '+(binding.view||'bytes'),'Server: '+server]});
   }
   edge(source,'task:'+t.id,binding.task?binding.output_port+' → '+binding.port:binding.port,t,'inputs',binding.port);
  }
  if(Object.keys(job.outputs).length){
   combos.push({id:'client',data:{title:'Result outlet',subtitle:job.output_route||'Union → Client'},style:{labelText:'Result outlet · '+(job.output_route||'Union → Client'),stroke:'#a3b2c5',fill:'#f5f7fa'}});
   details.set('client',{title:'Streaming result outlet',lines:[job.output_route||'Results forwarded through Union to the submitting client','Only execution metadata is shown; result streams are not consumed.']});
  }
  for(const [label,binding]of Object.entries(job.outputs)){
   const id='output:'+label;
  nodes.push({id,combo:'client',data:{kind:'output',title:label,subtitle:job.output_route||'Union → Client',color:'#6f8ba7',fill:'#fff',dash:[]},style:{size:[228,76]}});
   details.set(id,{title:'Output · '+label,lines:[binding.task+'.'+binding.port,job.output_route||'Forwarded through Union to the submitting client']});
   edge('task:'+binding.task,id,binding.port,tasks.get(binding.task),'outputs',binding.port);
  }
  if(job.logical){
   combos.length=0;
   for(const node of nodes){delete node.combo;node.data.logical=true;node.data.color='#5679a6';node.data.fill='#f4f8fd';node.data.dash=[];
    if(node.data.kind==='task'){const task=tasks.get(node.id.slice(5)),placement=task.placement;
     node.data.stateLabel=placement?compact(placement.node,34):'Logical step';
     if(placement){node.data.color='#2563eb';node.data.fill='#edf5ff';}
     const reasons={local_execution:'Execute on this server',data_locality:'Execute where the data resides',minimize_transfer:'Minimize estimated input transfer',upstream_locality:'Keep processing with upstream steps',available_capacity:'Use available node capacity',remote_read_capacity:'Read external data on an available member',explicit_placement:'Explicit internal placement'};
     details.set(node.id,{title:task.title||task.id,lines:['Step: '+task.id,'Operator: '+task.operator,...(placement?['Placement: '+placement.node,'Reason: '+(reasons[placement.reason]||placement.reason),...(placement.requested_source?['Source: '+placement.requested_source+' → '+placement.resolved_source]:[])]:[]),...Object.entries(task.params||{}).map(([key,value])=>key+': '+String(value))]});}
   }
   for(const edge of edges){edge.style.labelText=edge.data.port;edge.style.stroke='#94a7bd';details.set(edge.id,{title:'Stream dependency',lines:[edge.data.port]});}
   for(const id of [...details.keys()])if(id.startsWith('server:')||id==='client')details.delete(id);
  }
  const topology=JSON.stringify({nodes:nodes.map(n=>[n.id,n.combo]),edges:edges.map(e=>[e.id,e.source,e.target]),combos:combos.map(c=>c.id)});
  return {data:{nodes,edges,combos},details,topology};
 }
 // Swimlanes share global dependency columns; each server occupies a distinct row.
 function swimlane(data,vertical){
  const ranks=new Map(data.nodes.map(n=>[n.id,0])),remaining=new Map(data.nodes.map(n=>[n.id,0])),next=new Map(data.nodes.map(n=>[n.id,[]]));
  for(const e of data.edges){remaining.set(e.target,remaining.get(e.target)+1);next.get(e.source).push(e.target);}
  const queue=data.nodes.filter(n=>remaining.get(n.id)===0).map(n=>n.id);
  for(let i=0;i<queue.length;i++)for(const target of next.get(queue[i])){ranks.set(target,Math.max(ranks.get(target),ranks.get(queue[i])+1));remaining.set(target,remaining.get(target)-1);if(!remaining.get(target))queue.push(target);}
  const maxRank=Math.max(0,...ranks.values());let top=0;
  for(const combo of data.combos){
   const columns=new Map(),members=data.nodes.filter(n=>n.combo===combo.id);
   for(const n of members){const rank=ranks.get(n.id),slot=columns.get(rank)||0;columns.set(rank,slot+1);n.style={...n.style,x:vertical?top+170+slot*310:rank*400,y:vertical?rank*280:top+140+slot*190};}
   const left=Math.min(...members.map(n=>n.style.x-n.style.size[0]/2)),right=Math.max(...members.map(n=>n.style.x+n.style.size[0]/2));
   if(vertical){
    const minY=Math.min(...members.map(n=>n.style.y-n.style.size[1]/2)),maxY=Math.max(...members.map(n=>n.style.y+n.style.size[1]/2));
    combo.style.padding=[minY+145,30,maxRank*280+105-maxY,30];
   }else combo.style.padding=[64,maxRank*400+158-right,30,left+158];
   top+=Math.max(1,...columns.values())*(vertical?310:190)+110;
  }
 }
 function registerCard(){
  class TaskCard extends G6.Rect {
   render(attributes=this.parsedAttributes,container){
    super.render(attributes,container);
    const d=attributes.card||{},task=d.kind==='task',width=task?268:228,height=task?150:76,left=-width/2+18,top=-height/2;
    const text=(key,value,x,y,size,fill,weight=400)=>this.upsert(key,'text',{x,y,text:value,fontFamily:'system-ui, sans-serif',fontSize:size,fill,fontWeight:weight,textBaseline:'middle'},container);
    this.upsert('accent','rect',{x:-width/2,y:top+14,width:3,height:height-28,radius:2,fill:d.color},container);
    text('title',compact(d.title,task?19:22),left,top+25,14,'#243b53',600);
    text('subtitle',compact(d.subtitle,task?33:29),left,top+47,10,'#8291a5');
    if(task){
     text('status',d.stateLabel,left,top+73,11,d.color,600);
     this.upsert('divider','line',{x1:left,x2:width/2-18,y1:top+91,y2:top+91,stroke:'#e1e9f0',lineWidth:1},container);
     text('input-label',d.logical?'Inputs':'Input',left,top+109,10,'#8291a5');text('output-label',d.logical?'Outputs':'Output',12,top+109,10,'#8291a5');
     text('input',d.logical?'Stream':d.incoming,left,top+130,16,'#324c65',600);text('output',d.logical?'Stream':d.outgoing,12,top+130,16,'#324c65',600);
    }
   }
  }
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
   button('dag-expand',async()=>{for(const combo of this.graph.getComboData())if(combo.style?.collapsed)await this.graph.expandElement(combo.id,{animation:false});await this.fit();});
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
   if(!info){box.textContent='Click a task, input or edge for full details.';return;}
   const title=document.createElement('strong');title.textContent=info.title;box.append(title);
   for(const line of info.lines){const row=document.createElement('span');row.textContent=line;box.append(row);}
  }
  update(job,previous,fmt){
   this.lastRequest=[job,previous,fmt];
   const layout=this.layoutMode,vertical=this.direction==='vertical',layoutKey=layout+':'+this.direction,model=buildData(job,previous,fmt);
   if(layout==='swimlane')swimlane(model.data,vertical);
   if(vertical)for(const edge of model.data.edges)edge.style.targetPort='in';
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
      layout:layout==='swimlane'?undefined:{type:'combo-combined',comboPadding:[64,24,24,24],comboSpacing:64,layout:comboId=>({type:'dagre',rankdir:vertical?'TB':'LR',nodesep:comboId?36:70,ranksep:comboId?70:90})},
      node:{type:'dacp-task-card',state:{selected:{stroke:'#2563eb',lineWidth:3.5,shadowColor:'#2563eb66',shadowBlur:20,shadowOffsetY:0}},style:{card:d=>d.data,fill:d=>d.data.fill,stroke:d=>d.data.color,lineDash:d=>d.data.dash,lineWidth:d=>d.data.border||1.2,radius:12,shadowColor:'#1e3a5f0c',shadowBlur:10,shadowOffsetY:4,label:false,ports:[{key:'in',placement:vertical?'top':'left'},{key:'out',placement:vertical?'bottom':'right'},{key:'left',placement:'left'},{key:'right',placement:'right'}],portR:3,portFill:'#fff',portStroke:'#b5c4d6'}},
      edge:{type:'polyline',state:{selected:{stroke:'#2563eb',lineWidth:4,shadowColor:'#2563eb55',shadowBlur:10,labelFill:'#1d4ed8',labelFontWeight:700,labelBackgroundFill:'#dbeafe'}},style:{sourcePort:'out',targetPort:d=>d.style?.targetPort||'in',router:d=>({type:'shortest-path',enableObstacleAvoidance:true,offset:16,gridSize:10,startDirections:[vertical?'bottom':'right'],endDirections:[vertical?'top':'left']}),radius:10,endArrow:true,endArrowSize:7,labelFontFamily:'system-ui, sans-serif',labelFontSize:10,labelFill:'#6e829b',labelBackground:true,labelBackgroundFill:'#fff',labelBackgroundRadius:5,labelPadding:[4,7],labelLineHeight:15,labelAutoRotate:false,labelPlacement:'center'}},
      combo:{type:'rect',state:{selected:{stroke:'#2563eb',lineWidth:3.5,shadowColor:'#2563eb44',shadowBlur:16}},style:{radius:18,padding:d=>d.style?.padding||[64,24,24,24],lineWidth:2,fillOpacity:1,labelPlacement:'top-left',labelTextAlign:'left',labelOffsetX:16,labelOffsetY:20,labelFontSize:12,labelFontWeight:600,labelFill:'#526b88',collapsedSize:[268,76]}},
      behaviors:[{type:'drag-canvas',onFinish:()=>{this.mode='manual';}},{type:'zoom-canvas',enable:()=>this.mouseZoom,preventDefault:false,sensitivity:.5,onFinish:()=>{this.mode='manual';this.zoomLabel();}},{type:'collapse-expand',enable:event=>layout!=='swimlane'&&event.targetType==='combo',animation:false}],
     });
     for(const type of ['node','edge','combo'])this.graph.on(type+':click',event=>this.select(event.target.id));
     this.graph.on('canvas:click',()=>this.select(null));
     this.graph.on('aftertransform',()=>this.zoomLabel());
     await this.graph.render();if(selected)await this.graph.setElementState({[selected]:['selected']},false);this.ready=true;await this.fit();
    }else{
     // Retain G6-computed coordinates, edge routing, collapsed groups and viewport.
     this.graph.updateData({nodes:model.data.nodes.map(({style,...node})=>node),edges:model.data.edges.map(e=>({id:e.id,data:e.data,style:{labelText:e.style.labelText,stroke:e.style.stroke,lineWidth:e.style.lineWidth,lineDash:e.style.lineDash}}))});
     await this.graph.draw();if(this.inspected)this.inspect(this.inspected);
    }
    this.zoomLabel();
   });
   this.pending=work;return work;
  }
  destroyGraph(){this.ready=false;if(this.graph){this.graph.destroy();this.graph=null;}}
  destroy(){this.resize.disconnect();this.destroyGraph();}
 }
 return {Viewer,buildData};
})();

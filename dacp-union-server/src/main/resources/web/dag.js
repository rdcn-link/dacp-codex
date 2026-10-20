/* DACP execution viewer. G6 owns rendering, layout, grouping and viewport interaction. */
'use strict';
window.DacpDag=(()=>{
 const palette=[{stroke:'#2563eb',fill:'#dbeafe'},{stroke:'#c2410c',fill:'#ffedd5'},{stroke:'#15803d',fill:'#dcfce7'},{stroke:'#9333ea',fill:'#f3e8ff'},{stroke:'#be123c',fill:'#ffe4e6'},{stroke:'#0e7490',fill:'#cffafe'}];
 const states={succeeded:{color:'#168264',fill:'#f0faf5',label:'✓ 已完成'},running:{color:'#2563eb',fill:'#eff6ff',label:'▶ 执行中'},failed:{color:'#cc4560',fill:'#fff3f5',label:'! 执行失败'},cancelled:{color:'#cc4560',fill:'#fff3f5',label:'! 已取消'},expired:{color:'#cc4560',fill:'#fff3f5',label:'! 已超时'},interrupted:{color:'#cc4560',fill:'#fff3f5',label:'! 服务中断'},unknown:{color:'#b68424',fill:'#fff9eb',label:'? 状态未知'}};
 const pending={color:'#94a3b8',fill:'#f8fafc',label:'○ 尚未执行',dash:[5,4]};
 const compact=(text,max)=>Array.from(String(text||'')).length>max?Array.from(String(text)).slice(0,max-1).join('')+'…':String(text||'');
 function buildData(job,previous,fmt){
  const nodes=[],edges=[],combos=[],details=new Map(),tasks=new Map(job.tasks.map(t=>[t.id,t])),groups=new Map();
  for(const t of job.tasks)if(!groups.has(t.node)){
   const index=groups.size,id='server:'+index,color=palette[index%palette.length];groups.set(t.node,id);
   const count=job.tasks.filter(other=>other.node===t.node).length;
   combos.push({id,data:{title:t.node,subtitle:count+' 个步骤',...color},style:{labelText:t.node+'  ·  '+count+' 步',stroke:color.stroke,fill:color.fill}});
   details.set(id,{title:'执行服务器',lines:[t.node,count+' 个任务步骤','双击分组背景折叠 / 展开']});
  }
  function metric(task,direction,port){return job.nodes[task?.node_identity]?.ports?.[task?.id+'/'+direction+'/'+port];}
  function aggregate(task,direction){const all=Object.entries(job.nodes[task.node_identity]?.ports||{}).filter(([key])=>key.startsWith(task.id+'/'+direction+'/')).map(([,m])=>m);return all.length?fmt.bytes(all.reduce((sum,m)=>sum+(m.payload_bytes||0),0)):'—';}
  function flow(task,direction,port){
   const m=metric(task,direction,port);if(!m)return {label:'暂无观测',lines:['暂无流量观测'],remote:false};
   const observed=job.nodes[task.node_identity]?.observed_at_ms||0,last=previous.get(job.job_id+'/'+task.node_identity+'/'+task.id+'/'+direction+'/'+port),dt=observed-(last?.at||0);
   const rate=!fmt.terminal.has(job.state)&&last&&dt>0&&m.payload_bytes>=last.value?fmt.bytes((m.payload_bytes-last.value)*1000/dt)+'/s':'—';
   return {label:fmt.bytes(m.payload_bytes),remote:m.remote_payload_bytes>0,lines:['负载 '+fmt.bytes(m.payload_bytes),'速率 '+rate,'跨节点负载 '+fmt.bytes(m.remote_payload_bytes),...(m.records===undefined?[]:['记录数 '+m.records.toLocaleString()]),'最后活动 '+fmt.time(m.last_activity_ms)]};
  }
  function edge(source,target,label,task,direction,port){
   const traffic=flow(task,direction,port),id='flow:'+target+':'+port+':'+source;
   edges.push({id,source,target,data:{port:label,traffic:traffic.label},style:{targetPort:source.startsWith('task:')&&target.startsWith('task:')?'left':'in',labelText:label+'\n'+traffic.label,stroke:traffic.remote?'#5c7bb5':'#a3b2c5',lineWidth:traffic.remote?1.8:1.3}});
   details.set(id,{title:'数据流 · '+label,lines:traffic.lines});
  }
  for(const t of job.tasks){
   const state=states[t.state]||{...pending,label:pending.label+' · '+(fmt.labels[t.state]||t.state||'等待')};
   const incoming=aggregate(t,'inputs'),outgoing=aggregate(t,'outputs'),observed=job.nodes[t.node_identity]?.observed_at_ms;
   nodes.push({id:'task:'+t.id,combo:groups.get(t.node),data:{kind:'task',title:t.title||t.id,subtitle:t.operator,stateLabel:state.label,border:t.state==='running'?2:1.2,color:state.color,fill:state.fill,dash:state.dash||[],incoming,outgoing},style:{size:[268,150]}});
   details.set('task:'+t.id,{title:t.title||t.id,lines:['任务 ID：'+t.id,'算子：'+t.operator,'服务器：'+t.node,'状态：'+(fmt.labels[t.state]||t.state),'输入负载：'+incoming,'输出负载：'+outgoing,'最近观测：'+fmt.time(observed)]});
  }
  for(const t of job.tasks)for(const binding of t.inputs){
   let source='task:'+binding.task;
   if(!binding.task){
    source='input:'+t.id+':'+binding.port;
    nodes.push({id:source,combo:groups.get(t.node),data:{kind:'input',title:binding.uri?.split('/').pop()||binding.port,subtitle:(binding.remote?'跨节点输入':'本地输入')+' · '+binding.port,color:'#7f93ad',fill:'#fff',dash:[]},style:{size:[228,76]}});
    details.set(source,{title:(binding.remote?'跨节点输入':'本地输入')+' · '+binding.port,lines:[binding.uri||'', '表示：'+(binding.view||'bytes'),'服务器：'+t.node]});
   }
   edge(source,'task:'+t.id,binding.task?binding.output_port+' → '+binding.port:binding.port,t,'inputs',binding.port);
  }
  if(Object.keys(job.outputs).length){
   combos.push({id:'client',data:{title:'结果出口',subtitle:job.output_route||'Union → 客户端'},style:{labelText:'结果出口 · '+(job.output_route||'Union → 客户端'),stroke:'#a3b2c5',fill:'#f5f7fa'}});
   details.set('client',{title:'流式结果出口',lines:[job.output_route||'结果经 Union 转发至提交客户端','这里只展示执行元数据，不消费结果流。']});
  }
  for(const [label,binding]of Object.entries(job.outputs)){
   const id='output:'+label;
   nodes.push({id,combo:'client',data:{kind:'output',title:label,subtitle:job.output_route||'Union → 客户端',color:'#6f8ba7',fill:'#fff',dash:[]},style:{size:[228,76]}});
   details.set(id,{title:'输出 · '+label,lines:[binding.task+'.'+binding.port,job.output_route||'经 Union 转发至提交客户端']});
   edge('task:'+binding.task,id,binding.port,tasks.get(binding.task),'outputs',binding.port);
  }
  const topology=JSON.stringify({nodes:nodes.map(n=>[n.id,n.combo]),edges:edges.map(e=>[e.id,e.source,e.target]),combos:combos.map(c=>c.id)});
  return {data:{nodes,edges,combos},details,topology};
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
     text('input-label','输入',left,top+109,10,'#8291a5');text('output-label','输出',12,top+109,10,'#8291a5');
     text('input',d.incoming,left,top+130,16,'#324c65',600);text('output',d.outgoing,12,top+130,16,'#324c65',600);
    }
   }
  }
  G6.register(G6.ExtensionCategory.NODE,'dacp-task-card',TaskCard);
 }
 let registered=false;
 class Viewer {
  constructor(host){
   this.host=host;this.graph=null;this.ready=false;this.job=null;this.topology=null;this.details=new Map();this.inspected=null;this.pending=Promise.resolve();this.mode='fit';
   const button=(id,fn)=>document.getElementById(id).addEventListener('click',()=>this.action(fn));
   button('dag-zoom-in',async()=>{this.mode='manual';await this.graph.zoomTo(Math.min(2.5,this.graph.getZoom()*1.2),false);});
   button('dag-zoom-out',async()=>{this.mode='manual';await this.graph.zoomTo(Math.max(.15,this.graph.getZoom()/1.2),false);});
   button('dag-fit',()=>this.fit());
   button('dag-reset',async()=>{this.mode='manual';await this.graph.zoomTo(1,false);});
   button('dag-expand',async()=>{for(const combo of this.graph.getComboData())if(combo.style?.collapsed)await this.graph.expandElement(combo.id,{animation:false});await this.fit();});
   this.resize=new ResizeObserver(()=>{if(this.graph&&this.host.clientWidth&&this.host.clientHeight)this.action(async()=>{this.graph.resize(this.host.clientWidth,this.host.clientHeight);if(this.mode==='fit')await this.fit();});});
   this.resize.observe(host);
  }
  action(fn){
   this.pending=this.pending.catch(()=>{}).then(async()=>{if(this.graph){await fn();this.zoomLabel();}}).catch(error=>{const box=document.getElementById('dag-error');box.textContent='拓扑图操作失败：'+error.message;box.hidden=false;});return this.pending;
  }
  zoomLabel(){if(!this.graph||!this.ready)return;const zoom=this.graph.getZoom();document.getElementById('dag-zoom-label').textContent=Math.round(zoom*100)+'%';document.getElementById('dag-zoom-out').disabled=zoom<=.15;document.getElementById('dag-zoom-in').disabled=zoom>=2.5;}
  async fit(){this.mode='fit';await this.graph.fitView({when:'always',direction:'both'},false);this.zoomLabel();}
  inspect(id){
   this.inspected=id;const info=this.details.get(id),box=document.getElementById('dag-inspect');box.replaceChildren();
   if(!info){box.textContent='点击任务、输入或连线，查看完整信息。';return;}
   const title=document.createElement('strong');title.textContent=info.title;box.append(title);
   for(const line of info.lines){const row=document.createElement('span');row.textContent=line;box.append(row);}
  }
  update(job,previous,fmt){
   const model=buildData(job,previous,fmt);
   // Serialize viewport operations and refreshes; a slow render must finish before the next update.
   const work=this.pending.catch(()=>{}).then(async()=>{
    if(!window.G6)throw new Error('G6 图组件未加载');
    if(!registered){registerCard();registered=true;}
    this.details=model.details;
    const rebuild=!this.ready||this.job!==job.job_id||this.topology!==model.topology;
    if(rebuild){
     this.destroyGraph();this.host.replaceChildren();this.job=job.job_id;this.topology=model.topology;this.mode='fit';this.inspect(null);
     this.graph=new G6.Graph({
      container:this.host,width:this.host.clientWidth,height:this.host.clientHeight,animation:false,padding:36,zoomRange:[.15,2.5],data:model.data,
      layout:{type:'combo-combined',comboPadding:[64,24,24,24],comboSpacing:64,layout:comboId=>({type:'dagre',rankdir:'LR',nodesep:comboId?36:70,ranksep:comboId?70:90})},
      node:{type:'dacp-task-card',style:{card:d=>d.data,fill:d=>d.data.fill,stroke:d=>d.data.color,lineDash:d=>d.data.dash,lineWidth:d=>d.data.border||1.2,radius:12,shadowColor:'#1e3a5f0c',shadowBlur:10,shadowOffsetY:4,label:false,ports:[{key:'in',placement:'left'},{key:'out',placement:'right'},{key:'left',placement:'left'},{key:'right',placement:'right'}],portR:3,portFill:'#fff',portStroke:'#b5c4d6'}},
      edge:{type:'polyline',style:{sourcePort:'out',targetPort:d=>d.style?.targetPort||'in',router:d=>({type:'shortest-path',enableObstacleAvoidance:true,offset:16,gridSize:10,startDirections:['right'],endDirections:['left']}),radius:10,endArrow:true,endArrowSize:7,labelFontFamily:'system-ui, sans-serif',labelFontSize:10,labelFill:'#6e829b',labelBackground:true,labelBackgroundFill:'#fff',labelBackgroundRadius:5,labelPadding:[4,7],labelLineHeight:15,labelAutoRotate:false,labelPlacement:'center'}},
      combo:{type:'rect',style:{radius:18,padding:[64,24,24,24],lineWidth:2,fillOpacity:1,labelPlacement:'top-left',labelTextAlign:'left',labelOffsetX:16,labelOffsetY:20,labelFontSize:12,labelFontWeight:600,labelFill:'#526b88',collapsedSize:[268,76]}},
      behaviors:[{type:'drag-canvas',onFinish:()=>{this.mode='manual';}},{type:'zoom-canvas',sensitivity:.5,onFinish:()=>{this.mode='manual';this.zoomLabel();}},{type:'collapse-expand',enable:event=>event.targetType==='combo',animation:false}],
     });
     for(const type of ['node','edge','combo'])this.graph.on(type+':click',event=>this.inspect(event.target.id));
     this.graph.on('canvas:click',()=>this.inspect(null));
     this.graph.on('aftertransform',()=>this.zoomLabel());
     await this.graph.render();this.ready=true;await this.fit();
    }else{
     // Retain G6-computed coordinates, edge routing, collapsed groups and viewport.
     this.graph.updateData({nodes:model.data.nodes.map(({style,...node})=>node),edges:model.data.edges.map(e=>({id:e.id,data:e.data,style:{labelText:e.style.labelText,stroke:e.style.stroke,lineWidth:e.style.lineWidth}}))});
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

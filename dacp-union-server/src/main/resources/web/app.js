'use strict';
const $=id=>document.getElementById(id), terminal=new Set(['succeeded','failed','cancelled','expired','interrupted']);
const labels={interrupted:'服务中断',approved:'已批准',pending:'待批准',invited:'已邀请',revoked:'已撤销',rejected:'已拒绝',unknown:'未知',queued:'排队',waiting:'等待',waiting_resources:'等待资源',waiting_inputs:'等待输入',prepared:'已准备',preparing:'准备中',running:'运行中',succeeded:'成功',failed:'失败',cancelled:'已取消',expired:'已超时'};
let snapshot=null,selected=null,busy=false,historyOffset=0,previous=new Map();
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
const badge=s=>el('span',labels[s]||s||'未知','badge '+(['succeeded','approved'].includes(s)?'good':['failed','revoked','rejected'].includes(s)?'bad':['running','preparing','prepared','waiting_inputs'].includes(s)?'busy':'warn'));
const short=id=>id?id.slice(0,12)+(id.length>12?'…':''):'—';
const time=ms=>ms?new Date(ms).toLocaleString('zh-CN',{hour12:false}):'尚未观测';
const duration=ms=>ms<1000?ms+' ms':ms<60000?(ms/1000).toFixed(1)+' s':(ms/60000).toFixed(1)+' min';
function bytes(v){if(v===undefined||v===null)return '—';const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(v>=1024&&i<units.length-1){v/=1024;i++;}return v.toLocaleString('zh-CN',{maximumFractionDigits:i?1:0})+' '+units[i];}
function cell(row,value,cls){const td=el('td',undefined,cls);td.append(value instanceof Node?value:document.createTextNode(String(value)));row.append(td);return td;}
function empty(body,columns,text){const row=el('tr');cell(row,text,'empty').colSpan=columns;body.append(row);}
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('状态请求失败 ('+r.status+')');return r.json();}
function renderMembers(members,now){
 const body=$('members');body.replaceChildren();$('member-count').textContent=members.length+' 个成员';
 if(!members.length)return empty(body,9,'暂无成员。在 Union Server 上邀请节点，再由节点管理员批准。');
 for(const m of members){const row=el('tr'),title=el('div',m.url,'mono'),identity=el('div',short(m.identity),'muted mono');identity.title=m.identity;title.append(identity);cell(row,title);cell(row,badge(m.state));
  const stale=!m.observed_at_ms||now-m.observed_at_ms>Math.max(snapshot.poll_interval_ms*3,5000),s=m.status;
  cell(row,m.state==='approved'&&s&&!stale?el('span','可达','badge good'):el('span',stale?'观测过期':m.state==='unknown'?'不可达 / 未知':'尚未获准','badge warn'));
  cell(row,s?s.reserved_tasks+' / '+s.capacity:'—');cell(row,s?s.union_reserved_tasks+' / '+s.union_capacity:'—');cell(row,s&&s.executors?s.executors.join(', '):'—');cell(row,s?time(s.started_at_ms):'—');cell(row,s&&s.uptime_ms!==undefined?duration(s.uptime_ms):'—');cell(row,time(m.observed_at_ms));body.append(row);
 }
}
function renderJobs(){
 if(!snapshot)return;const body=$('jobs');body.replaceChildren();const query=$('search').value.trim().toLowerCase(),filter=$('state-filter').value;
 const jobs=snapshot.jobs.filter(j=>(!filter||(filter==='active'?!terminal.has(j.state):j.state===filter))&&(!query||[j.job_id,...j.tasks.flatMap(t=>[t.node,t.operator,t.id,t.title||''])].join(' ').toLowerCase().includes(query)));
 if(!jobs.length)return empty(body,6,snapshot.jobs.length?'没有匹配的作业':'暂无作业。请通过 Flight 客户端提交协作任务。');
 for(const j of jobs){const row=el('tr',undefined,j.job_id===selected?'selected':'');const b=el('button',short(j.job_id),'job-link');b.title=j.job_id;b.addEventListener('click',()=>{selected=j.job_id;previous.clear();renderJobs();refresh();});cell(row,b);cell(row,badge(j.state));cell(row,j.tasks.length+' / '+new Set(j.tasks.map(t=>t.node_identity)).size);cell(row,time(j.created_at_ms));cell(row,duration(j.elapsed_ms));cell(row,short(j.owner),'mono').title=j.owner;body.append(row);}
}
const dagView={job:null,scale:1,mode:'fit',width:0,height:0};
function applyDagZoom(scale,anchor){
 const host=$('dag'),svg=host.querySelector('svg');if(!svg||!dagView.width)return;
 const old=dagView.scale,next=Math.max(.25,Math.min(3,scale)),point=anchor||{x:host.clientWidth/2,y:host.clientHeight/2};
 const x=(host.scrollLeft+point.x)/old,y=(host.scrollTop+point.y)/old;
 dagView.scale=next;svg.setAttribute('width',dagView.width*next);svg.setAttribute('height',dagView.height*next);
 host.scrollLeft=x*next-point.x;host.scrollTop=y*next-point.y;
 $('dag-zoom-label').textContent=Math.round(next*100)+'%';$('dag-zoom-out').disabled=next<=.25;$('dag-zoom-in').disabled=next>=3;
}
function fitDag(){dagView.mode='fit';applyDagZoom(Math.min(1,($('dag').clientWidth-32)/dagView.width));$('dag').scrollLeft=0;}
$('dag-zoom-in').addEventListener('click',()=>{dagView.mode='manual';applyDagZoom(dagView.scale*1.2);});
$('dag-zoom-out').addEventListener('click',()=>{dagView.mode='manual';applyDagZoom(dagView.scale/1.2);});
$('dag-fit').addEventListener('click',fitDag);
$('dag-reset').addEventListener('click',()=>{dagView.mode='manual';applyDagZoom(1);});
$('dag').addEventListener('wheel',e=>{if(!e.ctrlKey&&!e.metaKey)return;e.preventDefault();dagView.mode='manual';const rect=$('dag').getBoundingClientRect();applyDagZoom(dagView.scale*Math.exp(-e.deltaY*.002),{x:e.clientX-rect.left,y:e.clientY-rect.top});},{passive:false});
let dagDrag=null;
$('dag').addEventListener('pointerdown',e=>{if(e.button!==0||e.pointerType==='touch')return;dagDrag={id:e.pointerId,x:e.clientX,y:e.clientY,left:$('dag').scrollLeft,top:$('dag').scrollTop};$('dag').setPointerCapture(e.pointerId);$('dag').classList.add('dragging');e.preventDefault();});
$('dag').addEventListener('pointermove',e=>{if(!dagDrag||dagDrag.id!==e.pointerId)return;$('dag').scrollLeft=dagDrag.left+dagDrag.x-e.clientX;$('dag').scrollTop=dagDrag.top+dagDrag.y-e.clientY;});
for(const event of ['pointerup','pointercancel','lostpointercapture'])$('dag').addEventListener(event,()=>{dagDrag=null;$('dag').classList.remove('dragging');});
new ResizeObserver(()=>{if(dagView.mode==='fit'&&$('dag').clientWidth)fitDag();}).observe($('dag'));
function renderDag(j){
 const host=$('dag'),saved={left:host.scrollLeft,top:host.scrollTop},newJob=dagView.job!==j.job_id;host.replaceChildren();
 if(newJob){dagView.job=j.job_id;dagView.scale=1;dagView.mode='fit';}
 $('dag-overview').textContent=new Set(j.tasks.map(t=>t.node)).size+' 个执行节点 · '+j.tasks.length+' 个步骤 · '+Object.keys(j.outputs).length+' 路输出';
 const ns='http://www.w3.org/2000/svg',svg=document.createElementNS(ns,'svg');
 const nodes=[],edges=[],byId=new Map(),tasks=new Map(j.tasks.map(t=>[t.id,t]));
 function add(id,title,lines,state,tooltip){const n={id,title,lines,state,tooltip,level:0};nodes.push(n);byId.set(id,n);return n;}
 function metric(t,direction,port){return j.nodes[t?.node_identity]?.ports?.[t.id+'/'+direction+'/'+port];}
 function rate(t,direction,port,m){const node=j.nodes[t?.node_identity],last=previous.get(j.job_id+'/'+t?.node_identity+'/'+t?.id+'/'+direction+'/'+port),dt=(node?.observed_at_ms||0)-(last?.at||0);return !terminal.has(j.state)&&last&&dt>0&&m.payload_bytes>=last.value?bytes((m.payload_bytes-last.value)*1000/dt)+'/s':'—';}
 function flow(t,direction,port){const m=metric(t,direction,port);return m?[bytes(m.payload_bytes)+' · '+rate(t,direction,port,m),'跨节点 '+bytes(m.remote_payload_bytes)+(m.records===undefined?'':' · '+m.records.toLocaleString()+' 条')]:['尚无流量观测'];}
 function totals(t,direction){const entries=Object.entries(j.nodes[t.node_identity]?.ports||{}).filter(([k])=>k.startsWith(t.id+'/'+direction+'/')).map(([,m])=>m);if(!entries.length)return '—';return bytes(entries.reduce((sum,m)=>sum+(m.payload_bytes||0),0))+'（跨节点 '+bytes(entries.reduce((sum,m)=>sum+(m.remote_payload_bytes||0),0))+'）';}
 for(const t of j.tasks){const observed=j.nodes[t.node_identity]?.observed_at_ms;
  add('task:'+t.id,t.title||t.id,[(t.state==='succeeded'?'✓ 已完成':t.state==='running'?'▶ 正在执行':t.state==='unknown'?'? 状态未知':['failed','cancelled','expired','interrupted'].includes(t.state)?'! 已停止':'○ 尚未执行')+' · '+(labels[t.state]||t.state||'未知'),t.operator,t.node,'输入 '+totals(t,'inputs'),'输出 '+totals(t,'outputs'),'观测 '+(observed?time(observed):'暂无')],t.state,'任务 ID: '+t.id+'\n'+(t.title||'未设置标题')+'\n'+t.operator+'\n'+t.node).lane=t.node;
 }
 for(const t of j.tasks)for(const b of t.inputs){let source;
  if(b.task)source='task:'+b.task;
  else {source='input:'+t.id+':'+b.port;add(source,b.uri?.split('/').pop()||b.port,['输入数据',b.uri,'端口 '+b.port],'input',b.uri).lane=t.node;}
  edges.push({source,target:'task:'+t.id,lines:[b.task?b.output_port+' → '+b.port:b.port,...flow(t,'inputs',b.port)]});
 }
 for(const [label,b]of Object.entries(j.outputs)){const id='output:'+label,t=tasks.get(b.task);add(id,label,['最终输出','经 Union 转发至客户端'],'output',label).lane='结果出口 · Union → 客户端';edges.push({source:'task:'+b.task,target:id,lines:[b.port,...flow(t,'outputs',b.port)]});}
 // Level assignment is independent of submission order; the server validates acyclicity.
 for(let i=0;i<nodes.length;i++){let changed=false;for(const e of edges){const a=byId.get(e.source),b=byId.get(e.target);if(a&&b&&b.level<=a.level){b.level=a.level+1;changed=true;}}if(!changed)break;}
 const lanes=[...new Set(nodes.map(n=>n.lane))].map((name,index)=>({name,index,x:12+index*376})),laneMap=new Map(lanes.map(l=>[l.name,l]));
 for(const n of nodes){const lane=laneMap.get(n.lane);n.x=lane.x+16;n.height=n.id.startsWith('task:')?178:104;}
 let offset=100;
 for(let level=0;level<=Math.max(0,...nodes.map(n=>n.level));level++){
  let layerHeight=0;
  for(const lane of lanes){let y=offset;for(const n of nodes.filter(n=>n.lane===lane.name&&n.level===level)){n.y=y;y+=n.height+24;}layerHeight=Math.max(layerHeight,y-offset-24);}
  offset+=Math.max(0,layerHeight)+108;
 }
 const width=Math.max(360,lanes.length*376+8),height=Math.max(210,offset-70);svg.setAttribute('viewBox',`0 0 ${width} ${height}`);svg.setAttribute('width',width);svg.setAttribute('height',height);
 function shape(tag,attrs,text){const e=document.createElementNS(ns,tag);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;return e;}
 function caption(g,x,y,text,cls,limit=43){const label=shape('text',{x,y,class:cls},text.length>limit?text.slice(0,limit-1)+'…':text);label.append(shape('title',{},text));g.append(label);}
 for(const lane of lanes){const g=shape('g',{class:'dag-lane','data-node':lane.name}),accent=['#4b77d1','#0d9488','#8262c3'][lane.index%3];
  g.append(shape('rect',{x:lane.x,y:12,width:352,height:height-24,rx:16,class:'dag-lane-bg'}),shape('rect',{x:lane.x+1,y:13,width:350,height:76,rx:15,class:'dag-lane-header'}),shape('circle',{cx:lane.x+31,cy:42,r:15,fill:accent,opacity:.12}),shape('rect',{x:lane.x+23,y:35,width:16,height:6,rx:2,fill:'none',stroke:accent,'stroke-width':1.5}),shape('rect',{x:lane.x+23,y:44,width:16,height:6,rx:2,fill:'none',stroke:accent,'stroke-width':1.5}));
  const count=j.tasks.filter(t=>t.node===lane.name).length;
  caption(g,lane.x+55,40,count?'执行节点 '+String(lane.index+1).padStart(2,'0'):'结果出口','dag-lane-title',32);
  caption(g,lane.x+55,60,count?lane.name:'Union → 客户端','dag-lane-address',41);
  g.append(shape('rect',{x:lane.x+286,y:30,width:48,height:25,rx:12,fill:accent,opacity:.1}),shape('text',{x:lane.x+310,y:47,'text-anchor':'middle',class:'dag-lane-count',fill:accent},count?count+' 步':'STREAM'));svg.append(g);
 }
 for(const e of edges){const a=byId.get(e.source),b=byId.get(e.target);if(!a||!b)continue;
  const outgoing=edges.filter(edge=>edge.source===e.source),incoming=edges.filter(edge=>edge.target===e.target);
  const x=a.x+320*(outgoing.indexOf(e)+1)/(outgoing.length+1),y=a.y+a.height,tx=b.x+320*(incoming.indexOf(e)+1)/(incoming.length+1),ty=b.y,mid=(y+ty)/2;
  const g=shape('g',{class:'dag-connection','data-source':e.source,'data-target':e.target});g.append(shape('path',{d:`M ${x} ${y} C ${x} ${mid}, ${tx} ${mid}, ${tx} ${ty}`,class:'dag-edge'}),shape('path',{d:`M ${tx-4} ${ty-7} L ${tx} ${ty} L ${tx+4} ${ty-7}`,class:'dag-edge'}));
  const cx=(x+tx)/2,boxHeight=e.lines.length*15+12;
  g.append(shape('rect',{x:cx-90,y:mid-boxHeight/2,width:180,height:boxHeight,rx:6,class:'dag-flow-bg'}));
  e.lines.forEach((line,i)=>g.append(shape('text',{x:cx,y:mid-boxHeight/2+17+i*15,class:'dag-label dag-flow','text-anchor':'middle'},line)));svg.append(g);
 }
 for(const n of nodes){const task=tasks.get(n.id.replace(/^task:/,'')),isTask=n.id.startsWith('task:'),cls=n.state==='succeeded'?'dag-success':['failed','cancelled','expired','interrupted'].includes(n.state)?'dag-failed':n.state==='running'?'dag-running':n.state==='unknown'?'dag-unknown':isTask?'dag-pending':'';
  const color=n.state==='succeeded'?'#168260':n.state==='running'?'#2563eb':cls==='dag-failed'?'#be123c':cls==='dag-unknown'?'#b7791f':'#94a3b8';
  const g=shape('g',{class:'dag-group '+cls,'data-state':n.state,'data-node':n.lane,'data-id':n.id});
  g.append(shape('rect',{x:n.x,y:n.y,width:320,height:n.height,rx:12,class:'dag-node '+cls}),shape('title',{},n.tooltip||n.title),shape('rect',{x:n.x+1,y:n.y+15,width:3,height:n.height-30,rx:2,fill:isTask?color:'#b7c6d6'}));
  caption(g,n.x+17,n.y+26,n.title,'dag-title',22);
  if(isTask){
   caption(g,n.x+17,n.y+46,task.operator,'dag-operator',43);
   g.append(shape('rect',{x:n.x+17,y:n.y+57,width:286,height:25,rx:6,class:'dag-state-bg '+cls}));caption(g,n.x+26,n.y+74,n.lines[0],'dag-state',40);
   g.append(shape('line',{x1:n.x+17,x2:n.x+303,y1:n.y+94,y2:n.y+94,class:'dag-card-divider'}));
   for(const [i,direction]of ['inputs','outputs'].entries()){
    const entries=Object.entries(j.nodes[task.node_identity]?.ports||{}).filter(([k])=>k.startsWith(task.id+'/'+direction+'/')).map(([,m])=>m),x=n.x+17+i*148;
    caption(g,x,n.y+112,direction==='inputs'?'输入负载':'输出负载','dag-metric-label');
    caption(g,x,n.y+134,entries.length?bytes(entries.reduce((sum,m)=>sum+(m.payload_bytes||0),0)):'—','dag-metric-value');
    caption(g,x,n.y+151,entries.length?'跨节点 '+bytes(entries.reduce((sum,m)=>sum+(m.remote_payload_bytes||0),0)):'暂无观测','dag-label');
   }
   caption(g,n.x+17,n.y+169,'更新 '+(j.nodes[task.node_identity]?.observed_at_ms?time(j.nodes[task.node_identity].observed_at_ms):'暂无观测'),'dag-timestamp',48);
  }else{caption(g,n.x+17,n.y+48,n.state==='input'?'本地输入数据':'流式结果输出','dag-kind');caption(g,n.x+17,n.y+71,n.lines[1]||'','dag-label',43);if(n.lines[2])caption(g,n.x+17,n.y+90,n.lines[2],'dag-label');}
  svg.append(g);
 }

 host.append(svg);dagView.width=width;dagView.height=height;
 if(dagView.mode==='fit')fitDag();else applyDagZoom(dagView.scale);
 host.scrollLeft=newJob?0:saved.left;host.scrollTop=newJob?0:saved.top;
}
function renderDetail(j){
 $('detail').hidden=false;$('job-script').textContent=j.job_yaml||'此历史记录未保存任务脚本';renderDag(j);
 $('detail').hidden=false;$('job-title').textContent=j.job_id;$('job-meta').textContent=(labels[j.state]||j.state)+' · 已用 '+duration(j.elapsed_ms)+' · 提交于 '+time(j.created_at_ms)+(j.finished_at_ms?' · 结束于 '+time(j.finished_at_ms):'');
 $('job-message').hidden=!j.message;$('job-message').textContent=j.message||'';$('events').replaceChildren(...(j.events||[]).map(e=>el('span',(labels[e.state]||e.state)+' · '+time(e.at_ms)+'  →  ')));
 const tasks=$('tasks');tasks.replaceChildren();
 for(const t of j.tasks){const card=el('article',undefined,'task'),head=el('div',undefined,'task-head');head.append(el('strong',t.title||t.id),badge(t.state));card.append(head,el('p',t.id+' · '+t.operator,'mono'),el('p',t.node,'muted mono'));const inputs=el('div',undefined,'dependency');for(const b of t.inputs)inputs.append(el('div',b.task?b.task+'.'+b.output_port+' → '+t.id+'.'+b.port:b.uri+' → '+t.id+'.'+b.port));card.append(inputs);tasks.append(card);}
 const ports=$('ports');ports.replaceChildren();let count=0;const nextPrevious=new Map();
 for(const [identity,node]of Object.entries(j.nodes)){const address=j.tasks.find(t=>t.node_identity===identity)?.node||short(identity);
  for(const [key,m]of Object.entries(node.ports||{})){const [task,direction,port]=key.split('/'),row=el('tr');cell(row,el('div',address+' / '+task,'mono'));cell(row,(direction==='inputs'?'输入':'输出')+' / '+port);cell(row,bytes(m.payload_bytes));cell(row,bytes(m.remote_payload_bytes));cell(row,m.records===undefined?'—':m.records.toLocaleString());
   const metricKey=j.job_id+'/'+identity+'/'+key,last=previous.get(metricKey),observed=node.observed_at_ms||0,dt=observed-(last?.at||observed),delta=m.payload_bytes-(last?.value||0);
   cell(row,terminal.has(j.state)?'—':dt>0&&delta>=0?bytes(delta*1000/dt)+'/s':'—');cell(row,time(m.last_activity_ms));ports.append(row);count++;nextPrevious.set(metricKey,{at:observed,value:m.payload_bytes});
  }
 }
 previous=nextPrevious;if(!count)empty(ports,7,'尚无端口流量，任务可能正在等待或准备。');
 $('outputs').replaceChildren(...Object.entries(j.outputs).map(([label,b])=>el('p',b.task+'.'+b.port+' → '+label+' → 提交客户端（经 U 转发）','mono')));
}
async function render(data){snapshot=data;$('union-info').textContent='Union '+data.union_id+' · '+data.flight_uri;const summary=$('summary');summary.replaceChildren();
 const healthy=data.members.filter(m=>m.state==='approved'&&m.status&&data.at_ms-m.observed_at_ms<=Math.max(data.poll_interval_ms*3,5000)).length;
 for(const [value,label]of [[data.members.length,'成员节点'],[healthy,'已批准且可达'],[data.active_jobs,'执行中的作业'],[data.succeeded_jobs,'窗口内成功作业']]){const stat=el('div',undefined,'stat');stat.append(el('strong',String(value)),el('span',label));summary.append(stat);}
 $('retention').textContent=$('job-view').value==='history'?'历史记录 '+(historyOffset+1)+'–'+(historyOffset+data.jobs.length)+' · 永久保留执行元数据':'最近 '+data.jobs.length+' / '+data.job_count+' 个作业 · 完成会话保留 '+data.retention_seconds+' 秒';renderMembers(data.members,data.at_ms);renderJobs();if(selected){const job=data.jobs.find(j=>j.job_id===selected);if(job){const id=selected;const detail=await api('/api/jobs/'+encodeURIComponent(id));if(selected===id)renderDetail(detail);}else{selected=null;$('detail').hidden=true;previous.clear();}}$('updated').textContent=time(data.at_ms);$('connection').textContent='已连接';$('error').hidden=true;
}
async function refresh(){if(busy)return;busy=true;try{const data=await api('/api/overview');if($('job-view').value==='history'){const page=await api('/api/history?offset='+historyOffset+'&limit=100');data.jobs=page.jobs;$('previous').disabled=historyOffset===0;$('next').disabled=page.jobs.length<100;}await render(data);}catch(e){$('error').textContent=e.message+'。页面保留上次快照，未获得最新状态。';$('error').hidden=false;$('connection').textContent='连接中断';}finally{busy=false;}}
$('refresh').addEventListener('click',refresh);$('job-view').addEventListener('change',()=>{historyOffset=0;selected=null;$('detail').hidden=true;for(const id of ['previous','next'])$(id).hidden=$('job-view').value!=='history';refresh();});$('previous').addEventListener('click',()=>{historyOffset=Math.max(0,historyOffset-100);refresh();});$('next').addEventListener('click',()=>{historyOffset+=100;refresh();});$('search').addEventListener('input',renderJobs);$('state-filter').addEventListener('change',renderJobs);
$('close-detail').addEventListener('click',()=>{selected=null;previous.clear();$('detail').hidden=true;renderJobs();});
refresh();setInterval(refresh,2000);

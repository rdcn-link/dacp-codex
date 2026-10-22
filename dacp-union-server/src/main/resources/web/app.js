'use strict';
const $=id=>document.getElementById(id), terminal=new Set(['succeeded','failed','cancelled','expired','interrupted']);
const labels={retrying:'重试中',replaying:'重放中',skipped:'已跳过',interrupted:'服务中断',approved:'已批准',pending:'待批准',invited:'已邀请',revoked:'已撤销',rejected:'已拒绝',unknown:'未知',queued:'排队',waiting:'等待',waiting_resources:'等待资源',waiting_inputs:'等待输入',prepared:'已准备',preparing:'准备中',running:'运行中',succeeded:'成功',failed:'失败',cancelled:'已取消',expired:'已超时'};
let snapshot=null,selected=null,busy=false,historyOffset=0,previous=new Map();
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
const badge=s=>el('span',labels[s]||s||'未知','badge '+(['succeeded','approved'].includes(s)?'good':['failed','revoked','rejected'].includes(s)?'bad':['running','preparing','prepared','waiting_inputs','retrying','replaying'].includes(s)?'busy':'warn'));
const short=id=>id?id.slice(0,12)+(id.length>12?'…':''):'—';
const time=ms=>ms?new Date(ms).toLocaleString('zh-CN',{hour12:false}):'尚未观测';
const duration=ms=>ms<1000?ms+' ms':ms<60000?(ms/1000).toFixed(1)+' s':(ms/60000).toFixed(1)+' min';
function bytes(v){if(v===undefined||v===null)return '—';const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(v>=1024&&i<units.length-1){v/=1024;i++;}return v.toLocaleString('zh-CN',{maximumFractionDigits:i?1:0})+' '+units[i];}
function cell(row,value,cls){const td=el('td',undefined,cls);td.append(value instanceof Node?value:document.createTextNode(String(value)));row.append(td);return td;}
function empty(body,columns,text){const row=el('tr');cell(row,text,'empty').colSpan=columns;body.append(row);}
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('状态请求失败 ('+r.status+')');return r.json();}
function renderMembers(members,now){
 const body=$('members');body.replaceChildren();$('member-count').textContent=members.length+' 个成员';
 if(!members.length)return empty(body,11,'暂无成员。在 Union Server 上邀请节点，再由节点管理员批准。');
 for(const m of members){const row=el('tr'),title=el('div',m.url,'mono'),identity=el('div',short(m.identity),'muted mono');identity.title=m.identity;title.append(identity);cell(row,title);cell(row,badge(m.state));
  const stale=!m.observed_at_ms||now-m.observed_at_ms>Math.max(snapshot.poll_interval_ms*3,5000),s=m.status;
  cell(row,m.state==='approved'&&s&&!stale?el('span','可达','badge good'):el('span',stale?'观测过期':m.state==='unknown'?'不可达 / 未知':'尚未获准','badge warn'));
  cell(row,s?s.reserved_tasks+' / '+s.capacity:'—');cell(row,s?s.union_reserved_tasks+' / '+s.union_capacity:'—');cell(row,s&&s.executors?s.executors.join(', '):'—');cell(row,s?time(s.started_at_ms):'—');cell(row,s&&s.uptime_ms!==undefined?duration(s.uptime_ms):'—');cell(row,time(m.observed_at_ms));const directory=el('a','查看数据集','job-link member-datasets-link');directory.href='/members/'+encodeURIComponent(m.identity)+'/datasets';directory.target='_blank';directory.rel='noopener';cell(row,directory);
  let web=el('span',s?'未启用':'—','muted');
  if(s&&s.web_enabled&&s.web_url){try{const url=new URL(s.web_url);if(['http:','https:'].includes(url.protocol)&&!url.username&&!url.password){web=el('a',s.web_url,'job-link member-web-link');web.href=url.href;web.target='_blank';web.rel='noopener';web.title=stale?'上次观测的 Web 地址':'打开节点 Web 看板';}}catch(_){web=el('span','无效地址','muted');}}
  cell(row,web);body.append(row);
 }
}
function renderJobs(){
 if(!snapshot)return;const body=$('jobs');body.replaceChildren();const query=$('search').value.trim().toLowerCase(),filter=$('state-filter').value;
 const jobs=snapshot.jobs.filter(j=>(!filter||(filter==='active'?!terminal.has(j.state):j.state===filter))&&(!query||[j.job_id,...j.tasks.flatMap(t=>[t.node,t.operator,t.id,t.title||''])].join(' ').toLowerCase().includes(query)));
 if(!jobs.length)return empty(body,6,snapshot.jobs.length?'没有匹配的作业':'暂无作业。请通过 Flight 客户端提交协作任务。');
 for(const j of jobs){const row=el('tr',undefined,j.job_id===selected?'selected':'');const b=el('button',short(j.job_id),'job-link');b.title=j.job_id;b.addEventListener('click',()=>{selected=j.job_id;previous.clear();renderJobs();refresh();});cell(row,b);cell(row,badge(j.state));cell(row,j.tasks.length+' / '+new Set(j.tasks.map(t=>t.node_identity)).size);cell(row,time(j.created_at_ms));cell(row,duration(j.elapsed_ms));cell(row,short(j.owner),'mono').title=j.owner;body.append(row);}
}
const dagViewer=new DacpDag.Viewer($('dag'));
async function renderDag(j){
 $('dag-overview').textContent=new Set(j.tasks.map(t=>t.node)).size+' 个执行节点 · '+j.tasks.length+' 个步骤 · '+Object.keys(j.outputs).length+' 路输出';
 try {await dagViewer.update(j,previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}
 catch(e){$('dag-error').textContent='拓扑图绘制失败：'+e.message;$('dag-error').hidden=false;}
}
async function renderDetail(j){
 $('detail').hidden=false;$('job-script').textContent=j.job_yaml||'此历史记录未保存任务脚本';$('job-policy').textContent=JSON.stringify({execution_policy:j.execution_policy||{},effective_execution_policy:j.effective_execution_policy||{},max_retries:j.max_retries||0,retry_count:j.retry_count||0,replay_of:j.replay_of||null,attempts:j.attempts||[]},null,2);await renderDag(j);if(selected!==j.job_id)return;
 $('detail').hidden=false;$('job-title').textContent=j.job_id;$('job-meta').textContent=(labels[j.state]||j.state)+' · 重试 '+(j.retry_count||0)+' / '+(j.max_retries||0)+' · 已用 '+duration(j.elapsed_ms)+' · 提交于 '+time(j.created_at_ms)+(j.finished_at_ms?' · 结束于 '+time(j.finished_at_ms):'');
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
 $('retention').textContent=$('job-view').value==='history'?'历史记录 '+(historyOffset+1)+'–'+(historyOffset+data.jobs.length)+' · 永久保留执行元数据':'最近 '+data.jobs.length+' / '+data.job_count+' 个作业 · 完成会话保留 '+data.retention_seconds+' 秒';renderMembers(data.members,data.at_ms);renderJobs();if(selected){const job=data.jobs.find(j=>j.job_id===selected);if(job){const id=selected;const detail=await api('/api/jobs/'+encodeURIComponent(id));if(selected===id)await renderDetail(detail);}else{selected=null;$('detail').hidden=true;previous.clear();}}$('updated').textContent=time(data.at_ms);$('connection').textContent='已连接';$('error').hidden=true;
}
async function refresh(){if(busy)return;busy=true;try{const data=await api('/api/overview');if($('job-view').value==='history'){const page=await api('/api/history?offset='+historyOffset+'&limit=100');data.jobs=page.jobs;$('previous').disabled=historyOffset===0;$('next').disabled=page.jobs.length<100;}await render(data);}catch(e){$('error').textContent=e.message+'。页面保留上次快照，未获得最新状态。';$('error').hidden=false;$('connection').textContent='连接中断';}finally{busy=false;}}
$('refresh').addEventListener('click',refresh);$('job-view').addEventListener('change',()=>{historyOffset=0;selected=null;$('detail').hidden=true;for(const id of ['previous','next'])$(id).hidden=$('job-view').value!=='history';refresh();});$('previous').addEventListener('click',()=>{historyOffset=Math.max(0,historyOffset-100);refresh();});$('next').addEventListener('click',()=>{historyOffset+=100;refresh();});$('search').addEventListener('input',renderJobs);$('state-filter').addEventListener('change',renderJobs);
$('close-detail').addEventListener('click',()=>{selected=null;previous.clear();$('detail').hidden=true;renderJobs();});
refresh();setInterval(refresh,2000);

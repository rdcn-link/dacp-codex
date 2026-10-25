'use strict';
const $=id=>document.getElementById(id), terminal=new Set(['succeeded','failed','cancelled','expired','interrupted']);
const labels={retrying:'Retrying',replaying:'Replaying',skipped:'Skipped',interrupted:'Service interrupted',approved:'Approved',pending:'Pending approval',invited:'Invited',revoked:'Revoked',rejected:'Rejected',unknown:'Unknown',queued:'Queued',waiting:'Waiting',waiting_resources:'Waiting for resources',waiting_inputs:'Waiting for input',waiting_dependencies:'Waiting for predecessors',prepared:'Prepared',preparing:'Preparing',running:'Running',succeeded:'Succeeded',failed:'Failed',cancelled:'Cancelled',expired:'Timed out'};
let snapshot=null,selected=null,busy=false,historyOffset=0,previous=new Map();
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
const badge=s=>el('span',labels[s]||s||'Unknown','badge '+(['succeeded','approved'].includes(s)?'good':['failed','revoked','rejected'].includes(s)?'bad':['running','preparing','prepared','waiting_inputs','waiting_dependencies','retrying','replaying'].includes(s)?'busy':'warn'));
const short=id=>id?id.slice(0,12)+(id.length>12?'…':''):'—';
const time=ms=>ms?new Date(ms).toLocaleString('en-US',{hour12:false}):'Not observed yet';
const duration=window.DacpDuration;
function bytes(v){if(v===undefined||v===null)return '—';const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(v>=1024&&i<units.length-1){v/=1024;i++;}return v.toLocaleString('en-US',{maximumFractionDigits:i?1:0})+' '+units[i];}
function cell(row,value,cls){const td=el('td',undefined,cls);td.append(value instanceof Node?value:document.createTextNode(String(value)));row.append(td);return td;}
function empty(body,columns,text){const row=el('tr');cell(row,text,'empty').colSpan=columns;body.append(row);}
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Status request failed ('+r.status+')');return r.json();}
function renderJobs(){
 if(!snapshot)return;const body=$('jobs');body.replaceChildren();const query=$('search').value.trim().toLowerCase(),filter=$('state-filter').value;
 const jobs=snapshot.jobs.filter(j=>(!filter||(filter==='active'?!terminal.has(j.state):j.state===filter))&&(!query||[j.job_id,...j.tasks.flatMap(t=>[t.node,t.operator,t.id,t.title||''])].join(' ').toLowerCase().includes(query)));
 if(!jobs.length)return empty(body,6,snapshot.jobs.length?'No matching jobs':'No jobs yet. Submit a collaborative job through a Flight client.');
 for(const j of jobs){const row=el('tr',undefined,j.job_id===selected?'selected':'');const b=el('button',short(j.job_id),'job-link');b.title=j.job_id;b.addEventListener('click',()=>{selected=j.job_id;previous.clear();renderJobs();refresh();});cell(row,b);cell(row,badge(j.state));cell(row,j.tasks.length+' / '+new Set(j.tasks.map(t=>t.node_identity)).size);cell(row,time(j.created_at_ms));cell(row,duration(j.elapsed_ms));cell(row,short(j.owner),'mono').title=j.owner;body.append(row);}
}
const dagViewer=new DacpDag.Viewer($('dag'));
async function renderDag(j){
 $('dag-overview').textContent=new Set(j.tasks.map(t=>t.node)).size+' execution nodes · '+j.tasks.length+' steps · '+Object.keys(j.outputs).length+' outputs';
 try {await dagViewer.update(j,previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}
 catch(e){$('dag-error').textContent='Graph rendering failed: '+e.message;$('dag-error').hidden=false;}
}
async function renderDetail(j){
 $('detail').hidden=false;$('job-script').textContent=j.job_yaml||'No task script was saved for this historical record';$('job-policy').textContent=JSON.stringify({execution_policy:j.execution_policy||{},effective_execution_policy:j.effective_execution_policy||{},max_retries:j.max_retries||0},null,2);DacpAttempts.render($('job-attempts'),j);await renderDag(j);if(selected!==j.job_id)return;
 $('detail').hidden=false;$('job-title').textContent=j.job_id;$('job-meta').textContent=(labels[j.state]||j.state)+' · Retry '+(j.retry_count||0)+' / '+(j.max_retries||0)+' · used '+duration(j.elapsed_ms)+' · Submitted at '+time(j.created_at_ms)+(j.finished_at_ms?' · Finished at '+time(j.finished_at_ms):'');
 $('job-message').hidden=!j.message;$('job-message').textContent=j.message||'';$('events').replaceChildren(...(j.events||[]).map(e=>el('span',(labels[e.state]||e.state)+' · '+time(e.at_ms)+'  →  ')));
 const tasks=$('tasks');tasks.replaceChildren();
 for(const t of j.tasks){const card=el('article',undefined,'task'),head=el('div',undefined,'task-head');head.append(el('strong',t.title||t.id),badge(t.state));card.append(head,el('p',t.id+' · '+t.operator,'mono'),el('p',t.node,'muted mono'));const inputs=el('div',undefined,'dependency');for(const b of t.inputs)inputs.append(el('div',b.task?b.task+'.'+b.output_port+' → '+t.id+'.'+b.port:b.uri+' → '+t.id+'.'+b.port));card.append(inputs);tasks.append(card);}
 const ports=$('ports');ports.replaceChildren();let count=0;const nextPrevious=new Map();
 for(const [identity,node]of Object.entries(j.nodes)){const address=j.tasks.find(t=>t.node_identity===identity)?.node||short(identity);
  for(const [key,m]of Object.entries(node.ports||{})){const [task,direction,port]=key.split('/'),row=el('tr');cell(row,el('div',address+' / '+task,'mono'));cell(row,(direction==='inputs'?'Input':'Output')+' / '+port);cell(row,bytes(m.payload_bytes));cell(row,bytes(m.remote_payload_bytes));cell(row,m.records===undefined?'—':m.records.toLocaleString());
   const metricKey=j.job_id+'/'+identity+'/'+key,last=previous.get(metricKey),observed=node.observed_at_ms||0,dt=observed-(last?.at||observed),delta=m.payload_bytes-(last?.value||0);
   cell(row,terminal.has(j.state)?'—':dt>0&&delta>=0?bytes(delta*1000/dt)+'/s':'—');cell(row,time(m.last_activity_ms));ports.append(row);count++;nextPrevious.set(metricKey,{at:observed,value:m.payload_bytes});
  }
 }
 previous=nextPrevious;if(!count)empty(ports,7,'No port traffic yet; tasks may be waiting or preparing.');
 $('outputs').replaceChildren(...Object.entries(j.outputs).map(([label,b])=>el('p',b.task+'.'+b.port+' → '+label+' → Submitting client (via Union)','mono')));
}
async function render(data){snapshot=data;$('union-info').textContent='Union '+data.union_id+' · '+data.flight_uri;const summary=$('summary');summary.replaceChildren();
 const healthy=data.members.filter(m=>m.state==='approved'&&m.status&&data.at_ms-m.observed_at_ms<=Math.max(data.poll_interval_ms*3,5000)).length;
 for(const [value,label]of [[data.members.length,'Member nodes'],[healthy,'Approved and reachable'],[data.active_jobs,'Active jobs'],[data.succeeded_jobs,'Successful jobs in window']]){const stat=el('div',undefined,'stat');stat.append(el('strong',String(value)),el('span',label));summary.append(stat);}
 $('retention').textContent=$('job-view').value==='history'?'History '+(historyOffset+1)+'–'+(historyOffset+data.jobs.length)+' · Execution metadata retained permanently':'Recent '+data.jobs.length+' / '+data.job_count+' jobs · completed sessions retained for '+duration(data.retention_seconds*1000);renderJobs();if(selected){const job=data.jobs.find(j=>j.job_id===selected);if(job){const id=selected;const detail=await api('/api/jobs/'+encodeURIComponent(id));if(selected===id)await renderDetail(detail);}else{selected=null;$('detail').hidden=true;previous.clear();}}$('updated').textContent=time(data.at_ms);$('connection').textContent='Connected';$('error').hidden=true;
}
async function refresh(){if(busy)return;busy=true;try{const data=await api('/api/overview');if($('job-view').value==='history'){const page=await api('/api/history?offset='+historyOffset+'&limit=100');data.jobs=page.jobs;$('previous').disabled=historyOffset===0;$('next').disabled=page.jobs.length<100;}await render(data);}catch(e){$('error').textContent=e.message+'. Showing the previous snapshot; current status is unavailable.';$('error').hidden=false;$('connection').textContent='Disconnected';}finally{busy=false;}}
$('refresh').addEventListener('click',refresh);$('job-view').addEventListener('change',()=>{historyOffset=0;selected=null;$('detail').hidden=true;for(const id of ['previous','next'])$(id).hidden=$('job-view').value!=='history';refresh();});$('previous').addEventListener('click',()=>{historyOffset=Math.max(0,historyOffset-100);refresh();});$('next').addEventListener('click',()=>{historyOffset+=100;refresh();});$('search').addEventListener('input',renderJobs);$('state-filter').addEventListener('change',renderJobs);
$('close-detail').addEventListener('click',()=>{selected=null;previous.clear();$('detail').hidden=true;renderJobs();});
refresh();setInterval(refresh,2000);

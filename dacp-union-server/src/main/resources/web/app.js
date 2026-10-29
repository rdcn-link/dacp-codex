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
 const jobs=snapshot.jobs.filter(j=>(!filter||(filter==='active'?!terminal.has(j.state):j.state===filter))&&(!query||[j.name||'',j.job_id,...j.tasks.flatMap(t=>[t.node,t.operator,t.id,t.title||''])].join(' ').toLowerCase().includes(query)));
 if(!jobs.length)return empty(body,7,snapshot.jobs.length?'No matching jobs':'No jobs yet. Submit a collaborative job through a DACP client.');
 for(const j of jobs){const row=el('tr',undefined,j.job_id===selected?'selected':'');const b=el('a',j.name||'Unnamed job','job-link');b.title=j.job_id;b.href='/job?id='+encodeURIComponent(j.job_id);cell(row,b);cell(row,short(j.job_id),'mono').title=j.job_id;cell(row,badge('Direct client'));cell(row,'Union job');cell(row,badge(j.state));cell(row,time(j.created_at_ms));cell(row,duration(j.elapsed_ms));body.append(row);}
}

async function render(data){snapshot=data;$('union-info').textContent='Union '+data.union_id+' · '+data.flight_uri;const summary=$('summary');summary.replaceChildren();
 const healthy=data.members.filter(m=>m.state==='approved'&&m.status&&data.at_ms-m.observed_at_ms<=Math.max(data.poll_interval_ms*3,5000)).length;
 for(const [value,label]of [[data.members.length,'Member nodes'],[healthy,'Approved and reachable'],[data.active_jobs,'Active jobs'],[data.succeeded_jobs,'Successful jobs in window']]){const stat=el('div',undefined,'stat');stat.append(el('strong',String(value)),el('span',label));summary.append(stat);}
 $('retention').textContent=$('job-view').value==='history'?'History '+(historyOffset+1)+'–'+(historyOffset+data.jobs.length)+' · Execution metadata retained permanently':'Recent '+data.jobs.length+' / '+data.job_count+' jobs · completed sessions retained for '+duration(data.retention_seconds*1000);renderJobs();$('updated').textContent=time(data.at_ms);$('connection').textContent='Connected';$('error').hidden=true;
}
async function refresh(){if(busy)return;busy=true;try{const data=await api('/api/overview');if($('job-view').value==='history'){const page=await api('/api/history?offset='+historyOffset+'&limit=100');data.jobs=page.jobs;$('previous').disabled=historyOffset===0;$('next').disabled=page.jobs.length<100;}await render(data);}catch(e){$('error').textContent=e.message+'. Showing the previous snapshot; current status is unavailable.';$('error').hidden=false;$('connection').textContent='Disconnected';}finally{busy=false;}}
$('refresh').addEventListener('click',refresh);$('job-view').addEventListener('change',()=>{historyOffset=0;selected=null;for(const id of ['previous','next'])$(id).hidden=$('job-view').value!=='history';refresh();});$('previous').addEventListener('click',()=>{historyOffset=Math.max(0,historyOffset-100);refresh();});$('next').addEventListener('click',()=>{historyOffset+=100;refresh();});$('search').addEventListener('input',renderJobs);$('state-filter').addEventListener('change',renderJobs);

refresh();setInterval(refresh,2000);

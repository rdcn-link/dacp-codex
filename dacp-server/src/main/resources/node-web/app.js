'use strict';
const $=id=>document.getElementById(id);
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
function cell(row,value){const td=el('td');td.append(value instanceof Node?value:document.createTextNode(String(value)));row.append(td);}
const time=ms=>ms?new Date(ms).toLocaleString('en-US',{hour12:false}):'—';
const duration=window.DacpDuration;
function bytes(n){if(n===undefined)return '—';const u=['B','KiB','MiB','GiB'];let i=0;while(n>=1024&&i<3){n/=1024;i++;}return n.toFixed(i?1:0)+' '+u[i];}
const terminal=new Set(['succeeded','failed','cancelled','expired','interrupted','rejected']);
const labels={retrying:'Retrying',replaying:'Replaying',skipped:'Skipped',accepted:'Accepted',prepared:'Prepared',running:'Running',waiting_inputs:'Waiting for input',waiting_dependencies:'Waiting for predecessors',succeeded:'Succeeded',failed:'Failed',cancelled:'Cancelled',expired:'Timed out',interrupted:'Service interrupted',unknown:'Unknown'};
let selected=null,offset=0,busy=false,previous=new Map();
const dagViewer=new DacpDag.Viewer($('dag'));
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Request failed ('+r.status+')');return r.json();}
function showError(e){$('error').textContent=e.message+'; current information is unavailable.';$('error').hidden=false;$('connection').textContent='Request failed';}
async function detail(){const id=selected;if(!id)return;const r=await api('/api/executions/'+encodeURIComponent(id));if(selected!==id)return;
 $('detail').hidden=false;$('execution-title').textContent=r.record_id;$('execution-state').textContent=(labels[r.state]||r.state)+' · '+r.kind;
 DacpAttempts.render($('execution-attempts'),r);
 $('policy').textContent=JSON.stringify({submitted:r.execution_policy||{},effective:r.effective_execution_policy||{}},null,2);
 $('script').textContent=r.job_yaml||JSON.stringify(r.task_definitions||r,null,2);
 const job={...r,tasks:r.graph_tasks,outputs:r.graph_outputs,nodes:r.graph_nodes};$('dag-overview').textContent=job.tasks.length+' steps';
 try{await dagViewer.update(job,previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}catch(e){$('dag-error').textContent='Graph rendering failed: '+e.message;$('dag-error').hidden=false;}
 const next=new Map();for(const [node,n]of Object.entries(job.nodes))for(const [key,m]of Object.entries(n.ports||{}))next.set(job.job_id+'/'+node+'/'+key,{at:n.observed_at_ms,value:m.payload_bytes});previous=next;
}
async function refresh(){if(busy)return;busy=true;try{
 const [info,page]=await Promise.all([api('/api/overview'),api('/api/history?offset='+offset+'&limit=50')]);
 $('address').textContent=info.flight_uri;$('summary').replaceChildren();
 for(const [value,label]of [[time(info.started_at_ms),'Started at'],[duration(info.uptime_ms),'Uptime'],[info.reserved_tasks+' / '+info.capacity,'Task capacity'],[info.executors.join(', '),'Executors']]){const stat=el('div',undefined,'stat');stat.append(el('strong',value),el('span',label));$('summary').append(stat);}
 const body=$('history');body.replaceChildren();for(const r of page.records){const row=el('tr');const button=el('button',r.task_id||r.execution_id||r.record_id,'job-link');button.title=r.record_id;button.addEventListener('click',()=>{selected=r.record_id;previous.clear();detail().catch(showError);});cell(row,button);cell(row,r.kind);cell(row,labels[r.state]||r.state);cell(row,time(r.created_at_ms));cell(row,duration(terminal.has(r.state)?r.elapsed_ms:Date.now()-r.created_at_ms));body.append(row);}
 $('page').textContent=' '+(offset+1)+'–'+(offset+page.records.length)+' ';$('previous').disabled=offset===0;$('next').disabled=page.records.length<50;
 await detail();$('connection').textContent='Connected';$('error').hidden=true;
 }catch(e){showError(e);}finally{busy=false;}}
async function datasets(){try{const data=await api('/api/datasets'),body=$('datasets');body.replaceChildren();for(const d of data.datasets){const row=el('tr');for(const v of [d.name,d.description,d.uri])cell(row,v);body.append(row);}if(!data.datasets.length){const r=el('tr');cell(r,'No datasets');body.append(r);}}catch(e){showError(e);}}
$('refresh').addEventListener('click',()=>{refresh();datasets();});$('previous').addEventListener('click',()=>{offset=Math.max(0,offset-50);refresh();});$('next').addEventListener('click',()=>{offset+=50;refresh();});$('close-detail').addEventListener('click',()=>{selected=null;previous.clear();$('detail').hidden=true;});
refresh();datasets();setInterval(refresh,2000);

'use strict';
const $=id=>document.getElementById(id);
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
function cell(row,value){const td=el('td');td.append(value instanceof Node?value:document.createTextNode(String(value)));row.append(td);return td;}
const time=ms=>ms?new Date(ms).toLocaleString('en-US',{hour12:false}):'—';
const duration=window.DacpDuration;
function bytes(n){if(n===undefined)return '—';const u=['B','KiB','MiB','GiB'];let i=0;while(n>=1024&&i<3){n/=1024;i++;}return n.toFixed(i?1:0)+' '+u[i];}
const terminal=new Set(['succeeded','failed','cancelled','expired','interrupted','rejected']);
const recordTypes={dacp_exchange:'User job',job_task:'Operator step',union_execution:'Union sub-job',union_task:'Operator step'};
const labels={retrying:'Retrying',replaying:'Replaying',skipped:'Skipped',accepted:'Accepted',prepared:'Prepared',running:'Running',waiting_inputs:'Waiting for input',waiting_dependencies:'Waiting for predecessors',succeeded:'Succeeded',failed:'Failed',cancelled:'Cancelled',expired:'Timed out',interrupted:'Service interrupted',unknown:'Unknown'};
let selected=null,offset=0,busy=false,previous=new Map();
const dagViewer=new DacpDag.Viewer($('dag'));
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Request failed ('+r.status+')');return r.json();}
function showError(e){$('error').textContent=e.message+'; current information is unavailable.';$('error').hidden=false;$('connection').textContent='Request failed';}
async function detail(){const id=selected;if(!id)return;const r=await api('/api/executions/'+encodeURIComponent(id));if(selected!==id)return;
 $('detail').hidden=false;$('execution-title').textContent=r.name||'Unnamed job';$('execution-state').textContent=(labels[r.state]||r.state)+' · '+(recordTypes[r.kind]||r.kind);
 $('execution-origin').textContent='Job ID: '+(r.execution_id||r.record_id)+' · Source: '+(r.execution_source==='union'?'Union':'Direct client')+(r.union_id?' · Union: '+r.union_id:'')+(r.execution_id?' · Execution: '+r.execution_id:'')+(r.parent_record_id||r.parent_execution_id?' · Parent record: '+(r.parent_record_id||r.parent_execution_id):'');
 const steps=$('step-records');steps.replaceChildren();
 for(const step of r.step_records||[]){const row=el('tr');row.title=step.record_id;for(const value of [step.title||step.task_id||step.record_id,step.operator||'—',labels[step.state]||step.state,time(step.started_at_ms||step.created_at_ms),duration(terminal.has(step.state)?step.elapsed_ms:Date.now()-step.created_at_ms),step.attempt_id||'—'])cell(row,value);steps.append(row);}
 if(!steps.children.length){const row=el('tr');cell(row,'No operator step records').colSpan=6;steps.append(row);}
 DacpAttempts.render($('execution-attempts'),r);
 $('policy').textContent=r.policy_yaml||'{}\n';
 $('definition-title').textContent=r.task_definitions?'Sub-job definition':r.job_yaml?'Job script':'Task definition';
 $('script').textContent=DacpJobScript.format(r.definition_yaml);
 const job={...r,tasks:r.graph_tasks,outputs:r.graph_outputs,nodes:r.graph_nodes};$('dag-overview').textContent=job.tasks.length+' steps';
 try{await dagViewer.update(job,previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}catch(e){$('dag-error').textContent='Graph rendering failed: '+e.message;$('dag-error').hidden=false;}
 const next=new Map();for(const [node,n]of Object.entries(job.nodes))for(const [key,m]of Object.entries(n.ports||{}))next.set(job.job_id+'/'+node+'/'+key,{at:n.observed_at_ms,value:m.payload_bytes});previous=next;
}
async function refresh(){if(busy)return;busy=true;try{
 const [info,page]=await Promise.all([api('/api/overview'),api('/api/history?offset='+offset+'&limit=50&source='+$('execution-source').value)]);
 $('address').textContent=info.flight_uri;$('summary').replaceChildren();
 for(const [value,label]of [[time(info.started_at_ms),'Started at'],[duration(info.uptime_ms),'Uptime'],[info.reserved_tasks+' / '+info.capacity,'Task capacity'],[info.executors.join(', '),'Executors']]){const stat=el('div',undefined,'stat');stat.append(el('strong',value),el('span',label));$('summary').append(stat);}
 const body=$('history');body.replaceChildren();for(const r of page.records){const row=el('tr');const button=el('button',r.name||'Unnamed job','job-link');button.title=r.record_id;button.addEventListener('click',()=>{selected=r.record_id;previous.clear();detail().catch(showError);});cell(row,button);const identifier=r.execution_id||r.record_id;const idCell=el('span',identifier.slice(0,8),'mono');idCell.title=identifier;cell(row,idCell);const source=el('span',r.execution_source==='union'?'Union':'Direct client','badge '+(r.execution_source==='union'?'busy':'good'));cell(row,source);cell(row,recordTypes[r.kind]||r.kind);cell(row,labels[r.state]||r.state);cell(row,time(r.created_at_ms));cell(row,duration(terminal.has(r.state)?r.elapsed_ms:Date.now()-r.created_at_ms));body.append(row);}
 if(!page.records.length){const row=el('tr');cell(row,'No execution records for this source').colSpan=7;body.append(row);}
 $('page').textContent=' '+(offset+1)+'–'+(offset+page.records.length)+' ';$('previous').disabled=offset===0;$('next').disabled=page.records.length<50;
 await detail();$('connection').textContent='Connected';$('error').hidden=true;
 }catch(e){showError(e);}finally{busy=false;}}
async function datasets(){try{const data=await api('/api/datasets'),body=$('datasets');body.replaceChildren();for(const d of data.datasets){const row=el('tr');for(const v of [d.name,d.description,d.uri])cell(row,v);body.append(row);}if(!data.datasets.length){const r=el('tr');cell(r,'No datasets');body.append(r);}}catch(e){showError(e);}}
$('execution-source').addEventListener('change',()=>{offset=0;selected=null;previous.clear();$('detail').hidden=true;refresh();});
$('refresh').addEventListener('click',()=>{refresh();datasets();});$('previous').addEventListener('click',()=>{offset=Math.max(0,offset-50);refresh();});$('next').addEventListener('click',()=>{offset+=50;refresh();});$('close-detail').addEventListener('click',()=>{selected=null;previous.clear();$('detail').hidden=true;});
refresh();datasets();setInterval(refresh,2000);

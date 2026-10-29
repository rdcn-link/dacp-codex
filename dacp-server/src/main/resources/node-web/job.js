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
let selected=new URLSearchParams(location.search).get('id'),offset=0,busy=false,previous=new Map();
let activePlan='physical',planViews={logical:'dag',physical:'dag'},latestJob=null;
const dagViewer=new DacpDag.Viewer($('dag'));
async function renderDag(j){
 if(planViews[activePlan]!=='dag')return;
 const logical=activePlan==='logical';
 if(logical&&!j.logical_plan)return;
 const graph=logical?{...j.logical_plan,job_id:j.job_id+':logical',logical:true,nodes:{},tasks:j.logical_plan.tasks.map(t=>({...t,node:'Logical plan',placement:$('show-placements').checked?{node:j.tasks.find(p=>p.id===t.id)?.node||'Local server',reason:'local_execution'}:null})),output_route:'Job result'}:j;
 const union=j.execution_source==='union';
 $('plan-dag-title').textContent=logical?(union?'Received sub-job DAG':'Logical plan DAG'):(union?'Local execution plan DAG':'Physical plan DAG');
 $('dag-overview').textContent=(logical?'':new Set(j.tasks.map(t=>t.node)).size+' execution nodes · ')+graph.tasks.length+' steps · '+Object.keys(graph.outputs).length+' outputs';
 try {await dagViewer.update(graph,logical?new Map():previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}
 catch(e){$('dag-error').textContent='Graph rendering failed: '+e.message;$('dag-error').hidden=false;}
}
async function showPlan(){
 for(const name of ['logical','physical']){
  $(name+'-plan').hidden=name!==activePlan;
  $(name+'-plan-tab').setAttribute('aria-selected',String(name===activePlan));
  $(name==='logical'?'job-script':'resolved-job-script').hidden=planViews[name]!=='script';
 }
 document.querySelectorAll('[data-plan-view]').forEach(b=>{const [plan,view]=b.dataset.planView.split(':');b.setAttribute('aria-pressed',String(planViews[plan]===view));});
 document.querySelector('.placement-toggle').hidden=latestJob?.execution_source==='union'||planViews.logical!=='dag';
 const available=activePlan!=='logical'||!!latestJob?.logical_plan;
 $('logical-unavailable').hidden=!!latestJob?.logical_plan;
 $('plan-dag-workspace').hidden=planViews[activePlan]!=='dag'||!available;
 $(activePlan+'-dag-slot').append($('plan-dag-workspace'));
 document.querySelector('[data-dag-layout="swimlane"]').disabled=activePlan==='logical';
 if(activePlan==='logical'&&dagViewer.layoutMode==='swimlane')dagViewer.setLayout('layout','dependency');
 if(latestJob)await renderDag(latestJob);
}
$('show-placements').addEventListener('change',()=>{if(latestJob)renderDag(latestJob);});
document.querySelectorAll('[data-plan]').forEach(b=>b.addEventListener('click',()=>{activePlan=b.dataset.plan;showPlan();}));
document.querySelectorAll('[data-plan]').forEach(b=>b.addEventListener('keydown',event=>{if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;event.preventDefault();activePlan=event.key==='Home'?'logical':event.key==='End'?'physical':activePlan==='logical'?'physical':'logical';$(activePlan+'-plan-tab').focus();showPlan();}));
document.querySelectorAll('[data-plan-view]').forEach(b=>b.addEventListener('click',()=>{const [plan,view]=b.dataset.planView.split(':');activePlan=plan;planViews[plan]=view;showPlan();}));

async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Request failed ('+r.status+')');return r.json();}
function showError(e){$('error').textContent=e.message+'; current information is unavailable.';$('error').hidden=false;}
async function detail(){const id=selected;if(!id)return;const r=await api('/api/executions/'+encodeURIComponent(id));if(selected!==id)return;
 $('detail').hidden=false;$('execution-title').textContent=r.name||'Unnamed job';$('execution-state').textContent=(labels[r.state]||r.state)+' · '+(recordTypes[r.kind]||r.kind);
 $('execution-origin').textContent='Job ID: '+(r.execution_id||r.record_id)+' · Source: '+(r.execution_source==='union'?'Union':'Direct client')+(r.union_id?' · Union: '+r.union_id:'')+(r.execution_id?' · Execution: '+r.execution_id:'')+(r.parent_record_id||r.parent_execution_id?' · Parent record: '+(r.parent_record_id||r.parent_execution_id):'');
 $('execution-state').textContent+=' · used '+duration(r.elapsed_ms)+' · Submitted at '+time(r.created_at_ms)+(r.finished_at_ms?' · Finished at '+time(r.finished_at_ms):'');
 $('execution-message').hidden=!r.message;$('execution-message').textContent=r.message||'';
 const steps=$('step-records');steps.replaceChildren();
 for(const step of r.step_records||[]){const row=el('tr');row.title=step.record_id;for(const value of [step.title||step.task_id||step.record_id,step.operator||'—',labels[step.state]||step.state,time(step.started_at_ms||step.created_at_ms),duration(terminal.has(step.state)?step.elapsed_ms:Date.now()-step.created_at_ms),step.attempt_id||'—'])cell(row,value);steps.append(row);}
 if(!steps.children.length){const row=el('tr');cell(row,'No operator step records').colSpan=6;steps.append(row);}
 DacpAttempts.render($('execution-attempts'),r);
 $('policy').textContent=r.policy_yaml||'{}\n';
 $('job-script').textContent=r.logical_yaml?DacpJobScript.format(r.logical_yaml):'Original logical plan unavailable.';
 $('resolved-job-script').textContent=DacpJobScript.format(r.definition_yaml);
 $('logical-scope').textContent=r.execution_source==='union'?'The executable sub-job received from Union: local steps and external input interfaces. Global logical and physical plans are available on Union.':'Submitted operators and stream dependencies, independent of execution placement.';
 const union=r.execution_source==='union';
 $('logical-plan-tab').textContent=union?'Received sub-job':'Logical plan';
 $('physical-plan-tab').textContent=union?'Local execution plan':'Physical plan';
 document.querySelector('#logical-plan .plan-view-tabs').setAttribute('aria-label',union?'Received sub-job view':'Logical plan view');
 document.querySelector('#physical-plan .plan-view-tabs').setAttribute('aria-label',union?'Local execution plan view':'Physical plan view');
 $('physical-scope').textContent=union?'Local execution of the received Union sub-job, including status, external inputs and stream traffic. Global placement was determined by Union.':'The local execution plan, including execution status, external inputs and stream traffic.';
 document.querySelector('.placement-toggle').hidden=union||planViews.logical!=='dag';
 const job={...r,tasks:r.graph_tasks,outputs:r.graph_outputs,nodes:r.graph_nodes};latestJob=job;
 $('outputs').replaceChildren(...Object.entries(job.outputs||{}).map(([label,b])=>el('p',b.task+'.'+b.port+' → '+label,'mono')));
 await showPlan();
 const next=new Map();for(const [node,n]of Object.entries(job.nodes))for(const [key,m]of Object.entries(n.ports||{}))next.set(job.job_id+'/'+node+'/'+key,{at:n.observed_at_ms,value:m.payload_bytes});previous=next;
}

async function refreshDetail(){if(busy)return;busy=true;try{if(!selected)throw new Error('Missing job ID');await detail();$('error').hidden=true;}catch(e){showError(e);}finally{busy=false;}}
$('refresh').addEventListener('click',refreshDetail);refreshDetail();

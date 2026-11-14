'use strict';
const $=id=>document.getElementById(id), terminal=new Set(['succeeded','failed','cancelled','expired','interrupted']);
const labels={retrying:'Retrying',replaying:'Replaying',skipped:'Skipped',interrupted:'Service interrupted',approved:'Approved',pending:'Pending approval',invited:'Invited',revoked:'Revoked',rejected:'Rejected',unknown:'Unknown',queued:'Queued',waiting:'Waiting',waiting_resources:'Waiting for resources',waiting_inputs:'Waiting for input',waiting_dependencies:'Waiting for predecessors',prepared:'Prepared',preparing:'Preparing',running:'Running',succeeded:'Succeeded',failed:'Failed',cancelled:'Cancelled',expired:'Timed out'};
let snapshot=null,selected=new URLSearchParams(location.search).get('id'),busy=false,historyOffset=0,previous=new Map();
function el(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
const badge=s=>el('span',labels[s]||s||'Unknown','badge '+(['succeeded','approved'].includes(s)?'good':['failed','revoked','rejected'].includes(s)?'bad':['running','preparing','prepared','waiting_inputs','waiting_dependencies','retrying','replaying'].includes(s)?'busy':'warn'));
const short=id=>id?id.slice(0,12)+(id.length>12?'…':''):'—';
const time=ms=>ms?new Date(ms).toLocaleString('en-US',{hour12:false}):'Not observed yet';
const duration=window.DacpDuration;
function bytes(v){if(v===undefined||v===null)return '—';const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(v>=1024&&i<units.length-1){v/=1024;i++;}return v.toLocaleString('en-US',{maximumFractionDigits:i?1:0})+' '+units[i];}
async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Status request failed ('+r.status+')');return r.json();}
let activeSubjob=null,activePhysical='overview';
let activePlan='physical',planViews={logical:'dag',physical:'dag'},latestJob=null;
const dagViewer=new DacpDag.Viewer($('dag'));
async function renderDag(j){
 if(planViews[activeSubjob?activePhysical:activePlan]!=='dag')return;
 const logical=activePlan==='logical';
 if(logical&&!j.logical_plan)return;
 const graph=activeSubjob?subjobGraph(j,activeSubjob):logical?{...j.logical_plan,job_id:j.job_id+':logical',logical:true,nodes:{},steps:j.logical_plan.steps.map(t=>({...t,node:'Logical plan'})),output_route:'Job result'}:j;
 $('plan-dag-title').textContent=activeSubjob?'Dispatched sub-job · '+activeSubjob.node:logical?'Logical plan DAG':'Physical plan DAG';
 $('dag-overview').textContent=(logical?'':new Set(graph.steps.map(t=>t.node)).size+' execution nodes · ')+graph.steps.length+' steps · '+Object.keys(graph.outputs).length+' outputs';
 try {await dagViewer.update(graph,logical?new Map():previous,{bytes,time,duration,labels,terminal});$('dag-error').hidden=true;}
 catch(e){$('dag-error').textContent='Graph rendering failed: '+e.message;$('dag-error').hidden=false;}
}
async function showPlan(){
 activeSubjob=activePlan==='physical'&&activePhysical.startsWith('subjob-')?latestJob?.sub_jobs?.[Number(activePhysical.slice(7))]:null;
 const sub=!!activeSubjob;
 for(const name of ['submission','logical','physical','execution']){
  $(name+'-plan').hidden=name!==activePlan;
  $(name+'-plan-tab').setAttribute('aria-selected',String(name===activePlan));
 }
 $('physical-overview').hidden=sub;$('subjob-plan').hidden=!sub;
 document.querySelectorAll('[data-physical]').forEach(b=>b.setAttribute('aria-selected',String(b.dataset.physical===activePhysical)));
 const viewKey=sub?activePhysical:activePlan;
 for(const name of ['logical','physical'])$(name==='logical'?'job-script':'resolved-job-script').hidden=planViews[name]!=='script';
 if(sub){$('subjob-plan').setAttribute('aria-labelledby','physical-'+activePhysical);$('subjob-script').textContent=DacpJobScript.format(activeSubjob.definition_yaml);$('subjob-script').hidden=planViews[viewKey]!=='script';$('subjob-heading').textContent=activeSubjob.node;}
 document.querySelectorAll('[data-plan-view]').forEach(b=>{const [plan,view]=b.dataset.planView.split(':');b.setAttribute('aria-pressed',String(planViews[plan==='subjob'?activePhysical:plan]===view));});
 const available=activePlan!=='logical'||!!latestJob?.logical_plan;
 $('logical-unavailable').hidden=!!latestJob?.logical_plan;
 $('plan-dag-workspace').hidden=!['logical','physical'].includes(activePlan)||planViews[viewKey]!=='dag'||!available;
 if(!['logical','physical'].includes(activePlan))return;
 $(sub?'subjob-dag-slot':activePlan+'-dag-slot').append($('plan-dag-workspace'));
 document.querySelector('[data-dag-layout="swimlane"]').disabled=activePlan==='logical';
 if(activePlan==='logical'&&dagViewer.layoutMode==='swimlane')dagViewer.setLayout('layout','dependency');
 if(latestJob)await renderDag(latestJob);
}
$('physical-plan-tabs').addEventListener('click',e=>{const b=e.target.closest('[data-physical]');if(b){activePhysical=b.dataset.physical;showPlan();}});
$('physical-plan-tabs').addEventListener('keydown',e=>DacpExecutionInfo.tabKeys(e,'[data-physical]'));
document.querySelector('.plan-tabs').addEventListener('click',e=>{const b=e.target.closest('[data-plan]');if(b){activePlan=b.dataset.plan;showPlan();}});
document.querySelector('.plan-tabs').addEventListener('keydown',event=>{
 if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
 const buttons=[...document.querySelectorAll('.plan-tabs [data-plan]')],index=buttons.indexOf(document.activeElement);if(index<0)return;
 event.preventDefault();const next=event.key==='Home'?0:event.key==='End'?buttons.length-1:(index+(event.key==='ArrowRight'?1:-1)+buttons.length)%buttons.length;
 buttons[next].focus();buttons[next].click();
});
document.querySelector('.plan-panel').addEventListener('click',e=>{const b=e.target.closest('[data-plan-view]');if(!b)return;const [plan,view]=b.dataset.planView.split(':');activePlan=plan==='subjob'?'physical':plan;planViews[plan==='subjob'?activePhysical:plan]=view;showPlan();});
// Keep external producers outside the dispatched member's execution group.
function subjobGraph(j,subjob){
 const ids=new Set(subjob.steps||[]),all=new Map(j.steps.map(t=>[t.id,t]));
 const steps=j.steps.filter(t=>ids.has(t.id)).map(t=>({...t,inputs:t.inputs.map(b=>{
  if(!b.task||ids.has(b.task))return b;
  const producer=all.get(b.task);
  return {...b,task:undefined,remote:true,upstream_task:b.task,upstream_port:b.output_port,upstream_origin:producer?.node,uri:producer?.node,view:b.view||'stream'};
 })}));
 const outputs={};
 for(const [label,b]of Object.entries(j.outputs))if(ids.has(b.task))outputs[label]=b;
 for(const t of j.steps)if(!ids.has(t.id))for(const b of t.inputs)if(ids.has(b.task))outputs[b.task+':'+b.output_port]={task:b.task,port:b.output_port};
 return {...j,job_id:j.job_id+':subjob:'+subjob.node,steps,outputs,output_route:'Downstream consumers / Union'};
}
// Topologically order member sub-jobs; break member-level cycles by logical step order.
function orderSubjobs(j){
 const jobs=j.sub_jobs||[],owner=new Map(),position=new Map(j.steps.map((t,i)=>[t.id,i]));
 jobs.forEach((job,i)=>(job.steps||[]).forEach(step=>owner.set(step,i)));
 const next=jobs.map(()=>new Set()),incoming=jobs.map(()=>0),remaining=new Set(jobs.map((_,i)=>i));
 for(const t of j.steps)for(const b of t.inputs||[]){
  const from=owner.get(b.task),to=owner.get(t.id);
  if(from!==undefined&&to!==undefined&&from!==to&&!next[from].has(to)){next[from].add(to);incoming[to]++;}
 }
 const rank=i=>Math.min(...(jobs[i].steps||[]).map(step=>position.get(step)??Infinity),Infinity);
 const compare=(a,b)=>(rank(a)-rank(b))||a-b,result=[];
 while(remaining.size){
  const ready=[...remaining].filter(i=>incoming[i]===0).sort(compare);
  const i=ready.length?ready[0]:[...remaining].sort(compare)[0];
  remaining.delete(i);result.push(jobs[i]);for(const target of next[i])incoming[target]--;
 }
 return result;
}
function renderPlanning(j){
 const report=j.planning,summary=$('planning-summary');summary.replaceChildren();summary.hidden=!report?.search;
 if(report?.search){
  summary.append(el('strong','Plan estimates · '+report.provider),el('span',report.evaluated_candidates+' candidates evaluated · estimated critical path '+duration(report.estimated_completion_ms)+' · cross-node '+bytes(report.estimated_transfer_bytes)+' · '+report.estimated_processes+' processes'));
  summary.append(el('small','Estimates use metadata and configured costs. Fusion candidates are verified by each member before launch; actual execution units appear on the DAG.'));
  const details=el('details'),title=el('summary','Transfers and execution unit candidates');details.append(title);
  for(const u of report.execution_units||[])details.append(el('p',u.steps.join(' → ')+' · '+u.node+' · '+u.mode+' · '+(u.runtime||'Runtime unknown')));
  for(const e of report.edges||[])details.append(el('p',e.from+' → '+e.to+' · '+e.mode+' · '+bytes(e.estimated_bytes)+' · '+duration(e.estimated_transfer_ms)));
  for(const warning of report.warnings||[])details.append(el('p',warning,'muted'));summary.append(details);
 }

 $('resolved-job-script').textContent=DacpJobScript.format(j.job_yaml);
 const tabs=$('physical-plan-tabs');tabs.querySelectorAll('[data-subjob-tab]').forEach(b=>b.remove());
 const complete=tabs.querySelector('[data-physical="overview"]'),locations=[...new Set(j.steps.map(t=>t.node).filter(Boolean))].join(' · ');
 complete.replaceChildren(el('span','Complete plan','plan-tab-title'),el('span',locations||'Placement pending','plan-tab-subtitle'));complete.title=locations||'Placement pending';
 (j.sub_jobs||[]).forEach((sub,index)=>{
  const key='subjob-'+index;planViews[key]??='dag';
  const button=el('button');button.id='physical-'+key;button.dataset.physical=key;button.dataset.subjobTab='';button.setAttribute('role','tab');button.setAttribute('aria-controls','subjob-plan');
  button.append(el('span','Sub-job '+(index+1),'plan-tab-title'),el('span',sub.node,'plan-tab-subtitle'));button.title=sub.node;tabs.append(button);
 });
 if(activePhysical.startsWith('subjob-')&&!j.sub_jobs?.[Number(activePhysical.slice(7))])activePhysical='overview';
}
async function renderDetail(j){
 $('detail').hidden=false;j={...j,sub_jobs:orderSubjobs(j)};latestJob=j;$('job-script').textContent=j.submitted_job_yaml?DacpJobScript.format(j.submitted_job_yaml):'Original logical plan unavailable.';renderPlanning(j);$('job-policy').textContent=DacpExecutionInfo.effectivePolicyYaml(j.effective_execution_policy_yaml);$('submitted-policy').textContent=j.execution_policy_yaml||'{}\n';$('submitted-script').textContent=j.submitted_job_yaml?DacpJobScript.format(j.submitted_job_yaml):'Submitted document was not recorded.';$('submission-meta').textContent='Received at '+time(j.created_at_ms);DacpExecutionInfo.render($('execution-info'),j,{time,bytes,labels,terminal});DacpAttempts.render($('job-attempts'),j);await showPlan();if(selected!==j.job_id)return;
 $('detail').hidden=false;$('job-title').textContent=j.name||'Unnamed job';$('job-meta').textContent='Job ID: '+j.job_id+' · '+(labels[j.state]||j.state)+' · Retry '+(j.retry_count||0)+' / '+(j.max_retries||0)+' · Total '+duration(j.elapsed_ms)+(j.preparation_duration_ms!=null?' · Preparation '+duration(j.preparation_duration_ms):'')+(j.execution_duration_ms!=null?' · Execution '+duration(j.execution_duration_ms):'')+' · Submitted at '+time(j.created_at_ms)+(j.finished_at_ms?' · Finished at '+time(j.finished_at_ms):'');
 $('job-message').hidden=!j.message;$('job-message').textContent=j.message||'';$('events').replaceChildren(...(j.events||[]).map(e=>el('span',(labels[e.state]||e.state)+' · '+time(e.at_ms)+'  →  ')));
 const nextPrevious=new Map();
 for(const [identity,node]of Object.entries(j.nodes))for(const [key,m]of Object.entries(node.ports||{})){
  nextPrevious.set(j.job_id+'/'+identity+'/'+key,{at:node.observed_at_ms||0,value:m.payload_bytes});
 }
 previous=nextPrevious;
 $('outputs').replaceChildren(...Object.entries(j.outputs).map(([label,b])=>el('p',b.task+'.'+b.port+' → '+label+' → Submitting client (via Union)','mono')));
}

async function refreshDetail(){if(busy)return;busy=true;try{if(!selected)throw new Error('Missing job ID');await renderDetail(await api('/api/jobs/'+encodeURIComponent(selected)));$('connection').textContent='Connected';$('error').hidden=true;}catch(e){$('error').textContent=e.message;$('error').hidden=false;$('connection').textContent='Unavailable';}finally{busy=false;}}
$('refresh').addEventListener('click',refreshDetail);refreshDetail();

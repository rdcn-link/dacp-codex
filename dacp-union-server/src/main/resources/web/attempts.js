'use strict';
window.DacpAttempts={render(container,job){
 const open=new Set([...container.querySelectorAll('details[open]')].map(n=>n.dataset.attempt));
 if(container.dataset.job!==(job.job_id||job.record_id))open.clear();container.dataset.job=job.job_id||job.record_id||'';
 const make=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
 const states={running:'Running',preparing:'Preparing',waiting_resources:'Waiting for resources',succeeded:'Succeeded',failed:'Failed',cancelled:'Cancelled',expired:'Timed out',replaying:'Replaying'};
 const date=t=>t?new Date(t).toLocaleString('en-US',{hour12:false}):'Not recorded';
 const attempts=[...(job.attempts||[])];
 if(job.attempt_id&&!attempts.some(a=>a.attempt_id===job.attempt_id)&&!['succeeded','failed','cancelled','expired','interrupted'].includes(job.state))attempts.push({attempt_id:job.attempt_id,retry_index:job.retry_count||0,state:job.state,started_at_ms:job.attempt_started_at_ms,nodes:job.nodes});
 container.replaceChildren();
 if(job.replay_of)container.append(make('p','Replayed from: '+job.replay_of,'muted mono'));
 if(!attempts.length){container.append(make('p','No execution attempts recorded.','muted'));return;}
 for(const a of attempts){
  const detail=make('details',undefined,'attempt-card');detail.dataset.attempt=a.attempt_id;detail.open=open.has(a.attempt_id);
  const summary=make('summary');summary.append(make('strong','Attempt '+((a.retry_index||0)+1)),make('span',states[a.state]||a.state,'badge '+(a.state==='succeeded'?'good':['failed','cancelled','expired'].includes(a.state)?'bad':'busy')));
  const elapsed=a.started_at_ms?Math.max(0,(a.finished_at_ms||Date.now())-a.started_at_ms):null;
  summary.append(make('span',date(a.started_at_ms)+' → '+(a.finished_at_ms?date(a.finished_at_ms):['succeeded','failed','cancelled','expired'].includes(a.state)?'Not recorded':'In progress'),'muted'),make('span',elapsed===null?'Duration not recorded':(elapsed/1000).toFixed(1)+' s','attempt-duration'));
  detail.append(summary);const body=make('div',undefined,'attempt-body');body.append(make('p','Attempt ID: '+a.attempt_id,'muted mono'));
  if(a.message)body.append(make('p',a.message,'notice'));
  if(a.retry_index>0)body.append(make('p','Automatic retry after failure · retry '+a.retry_index,'muted'));
  let count=0;
  for(const [identity,node] of Object.entries(a.nodes||{})){
   const section=make('div',undefined,'attempt-node');section.append(make('strong',job.tasks?.find(t=>t.node_identity===identity)?.node||identity));
   for(const [task,state] of Object.entries(node.tasks||{})){const title=job.tasks?.find(t=>t.id===task)?.title||task;section.append(make('p',title+' · '+(states[state]||state)));count++;}
   body.append(section);
  }
  if(!count)body.append(make('p','No step snapshots were recorded for this attempt.','muted'));
  detail.append(body);container.append(detail);
 }
}};

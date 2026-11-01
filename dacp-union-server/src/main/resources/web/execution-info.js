/* Shared read-only execution summary for Union and member dashboards. */
'use strict';
window.DacpExecutionInfo=(()=>{
 const node=(tag,text,cls)=>{const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;};
 function tabKeys(event,selector){
  if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;
  const buttons=[...event.currentTarget.querySelectorAll(selector)],i=buttons.indexOf(document.activeElement);if(i<0)return;
  event.preventDefault();const index=event.key==='Home'?0:event.key==='End'?buttons.length-1:(i+(event.key==='ArrowRight'?1:-1)+buttons.length)%buttons.length;
  buttons[index].focus();buttons[index].click();
 }
 function render(container,job,fmt){
  container.replaceChildren();
  const members=new Map();
  for(const task of job.tasks||[]){const key=task.node_identity||task.node;if(!members.has(key))members.set(key,{address:task.node,tasks:[]});members.get(key).tasks.push(task);}
  for(const [identity,member] of members){
   const snapshot=job.nodes?.[identity]||{},card=node('section',undefined,'execution-member'),head=node('div',undefined,'section-head');
   head.append(node('h4',member.address||'Local execution'));card.append(head);
   const sub=(job.sub_jobs||[]).find(s=>s.node===member.address),record=snapshot.history_record_id;
   if(sub?.web_url&&record){
    try{const url=new URL('/job',sub.web_url);if(['http:','https:'].includes(url.protocol)){url.searchParams.set('id',record);const link=node('a','Open node details ↗','job-link');link.href=url.href;link.target='_blank';link.rel='noopener';head.append(link);}}catch(_){}
   }
   const state=snapshot.state||'unknown',observed=snapshot.observed_at_ms||job.updated_at_ms||0;
   const stale=snapshot.observation_error||(!fmt.terminal.has(job.state)&&observed&&Date.now()-observed>15000)||(fmt.terminal.has(job.state)&&snapshot.state&&!fmt.terminal.has(snapshot.state));
   const elapsed=snapshot.created_at_ms?Math.max(0,(snapshot.finished_at_ms||observed||snapshot.created_at_ms)-snapshot.created_at_ms):null;
   card.append(node('p',(fmt.labels[state]||state)+(elapsed!==null?' · Duration: '+window.DacpDuration(elapsed):'')+' · Last observation: '+fmt.time(observed)+(stale?' · Observation unavailable or incomplete; showing last known state':''),stale?'notice':'muted'));
   if(snapshot.message)card.append(node('p',snapshot.message,'notice'));
   const scroll=node('div',undefined,'table-scroll'),table=node('table'),header=node('tr');
   for(const label of ['Step','Status','Execution unit / runtime','Input payload','Output payload'])header.append(node('th',label));
   const thead=node('thead');thead.append(header);table.append(thead);const body=node('tbody');
   for(const task of member.tasks){
    const row=node('tr'),unit=(snapshot.execution_units||[]).find(u=>(u.steps||[]).includes(task.id));
    const runtime=unit?.runtime_name?unit.runtime_name+(unit.runtime_version?'@'+unit.runtime_version:''):unit?.mode==='host_stream_source'?'Host JVM':unit?.runtime_executor||'Not recorded';
    const value=direction=>{const metrics=Object.entries(snapshot.ports||{}).filter(([key])=>key.startsWith(task.id+'/'+direction+'/'));return metrics.length?fmt.bytes(metrics.reduce((sum,[,m])=>sum+(m.payload_bytes||0),0)):'—';};
    for(const text of [task.title||task.id,fmt.labels[task.state]||task.state||'Unknown',unit?unit.id+' · '+runtime:'Pending / not recorded',value('inputs'),value('outputs')])row.append(node('td',text));
    if(unit)row.title='Mode: '+unit.mode+(unit.reason?' · Reason: '+unit.reason:'')+(unit.runtime_image?' · Image: '+unit.runtime_image:'');
    body.append(row);
   }
   table.append(body);scroll.append(table);card.append(scroll);container.append(card);
  }
  if(!members.size)container.append(node('p','No physical execution records available.','muted'));
 }
 return {render,tabKeys};
})();

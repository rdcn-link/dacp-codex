'use strict';
const directoryNodes=document.getElementById('directory-nodes'),directoryError=document.getElementById('directory-error');
const nodeStates={approved:'Approved',pending:'Pending approval',invited:'Invited',revoked:'Revoked',rejected:'Rejected',unknown:'Unknown'};
let generation=0,healthWindow=10000,polling=false;
function element(tag,text,cls){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;}
async function request(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error(r.status===503?'Node is temporarily unreachable. Try again later.':'Read failed (HTTP '+r.status+'). Try again.');return r.json();}
function card(member,index,version){
 const box=element('article',undefined,'directory-node tone-'+index%4),heading=element('button',undefined,'directory-heading');box.dataset.identity=member.identity;
 heading.type='button';heading.setAttribute('aria-expanded','false');heading.setAttribute('aria-controls','node-directory-'+index);
 const icon=element('span',undefined,'server-glyph');icon.setAttribute('aria-hidden','true');for(let i=0;i<3;i++)icon.append(element('i'));
 const title=element('span',undefined,'directory-title');title.append(element('strong',member.url),element('span',member.identity,'muted mono'));
 const status=element('span',nodeStates[member.state]||member.state,'badge '+(member.state==='approved'?'good':'warn'));
 const action=element('span','Expand datasets ＋','directory-action');heading.append(icon,title,status,action);
 const body=element('div',undefined,'directory-body');body.id='node-directory-'+index;body.hidden=true;
 const facts=element('div',undefined,'node-facts');
 box.updateMember=current=>{
 member=current;status.textContent=nodeStates[member.state]||member.state;status.className='badge '+(member.state==='approved'?'good':'warn');
 facts.replaceChildren();
 const state=member.status,stale=!member.observed_at_ms||Date.now()-member.observed_at_ms>healthWindow;
 facts.append(element('span',member.state==='approved'&&state&&!stale?'● Reachable':'● Unknown or not approved','node-health'));
 facts.append(element('span','Task capacity: '+(state?state.reserved_tasks+' / '+state.capacity:'—')));
 facts.append(element('span','Union capacity: '+(state?state.union_reserved_tasks+' / '+state.union_capacity:'—')));
 const resource=state?.resources,percent=v=>v===undefined?'—':(v*100).toFixed(1)+'%';
 const gauge=(label,fraction,hint)=>{const item=element('span',undefined,'resource-gauge');item.append(element('strong',label+' '+percent(fraction)));const meter=element('meter');meter.min=0;meter.max=1;meter.value=fraction??0;meter.setAttribute('aria-label',label);if(fraction===undefined)meter.hidden=true;item.append(meter);item.title=hint;facts.append(item);};
 gauge('Environment CPU',resource?.cpu_load,'JVM CPU utilization of the visible host or container');
 gauge('Process CPU',resource?.process_cpu_load,'DACP JVM Process CPU utilization');
 gauge('Environment memory',resource?.memory_total_bytes?resource.memory_used_bytes/resource.memory_total_bytes:undefined,'Environment memory used / total; shared with other processes');
 gauge('JVM Heap',resource?.heap_max_bytes?resource.heap_used_bytes/resource.heap_max_bytes:undefined,'JVM Heap used / maximum; excludes other process memory');
 if(resource)facts.append(element('span','CPU cores '+resource.processors+' · JVM threads '+resource.threads));
 facts.append(element('span','Executors: '+(state?.executors?.join(', ')||'—')));
 facts.append(element('span','Started: '+(state?.started_at_ms?new Date(state.started_at_ms).toLocaleString('en-US'):'Not observed')));
 facts.append(element('span','Uptime: '+(state?.uptime_ms!==undefined?(state.uptime_ms/1000).toFixed(0)+' s':'Not observed')));
 if(state?.web_enabled&&state.web_url){try{const url=new URL(state.web_url);if(['http:','https:'].includes(url.protocol)&&!url.username&&!url.password){const link=element('a','Node web ↗','job-link member-web-link');link.href=url.href;link.target='_blank';link.rel='noopener';facts.append(link);}}catch(_){}}
 else facts.append(element('span',state?'Node web disabled':'Node web unknown'));
 facts.append(element('span','Observed: '+(member.observed_at_ms?new Date(member.observed_at_ms).toLocaleString('en-US'):'Not observed')));
 };
 box.updateMember(member);
 box.append(heading,facts,body);let loaded=false,loading=false;
 async function load(){
  if(loading)return;loading=true;body.replaceChildren(element('p','Reading the member catalog through Flight…','muted'));body.setAttribute('aria-busy','true');
  try{
   const data=await request('/members/'+encodeURIComponent(member.identity)+'/datasets');if(version!==generation)return;
   body.replaceChildren();const bar=element('div',undefined,'section-head');const refresh=element('button','Reload');refresh.type='button';refresh.addEventListener('click',load);
   bar.append(element('p',data.datasets.length+' datasets · '+new Date(data.observed_at_ms).toLocaleString('en-US'),'muted'),refresh);body.append(bar);
   if(!data.datasets.length)body.append(element('p','This node has no datasets','empty'));
   else{
    const scroll=element('div',undefined,'table-scroll'),table=element('table'),head=element('thead'),tr=element('tr');
    for(const label of ['name (local member name)','Description (JSON)','Node URI'])tr.append(element('th',label));head.append(tr);table.append(head);const rows=element('tbody');
    for(const dataset of data.datasets){const row=element('tr');row.append(element('td',dataset.name,'mono'));const description=element('td');description.append(element('pre',dataset.description,'dataset-description'));row.append(description,element('td',dataset.uri,'mono'));rows.append(row);}table.append(rows);scroll.append(table);body.append(scroll);
   }
   loaded=true;
  }catch(error){if(version!==generation)return;loaded=false;body.replaceChildren(element('p',error.message,'notice'));const retry=element('button','Retry');retry.type='button';retry.addEventListener('click',load);body.append(retry);}
  finally{loading=false;body.removeAttribute('aria-busy');}
 }
 heading.addEventListener('click',()=>{const open=body.hidden;body.hidden=!open;heading.setAttribute('aria-expanded',String(open));action.textContent=open?'Collapse −':'Expand datasets ＋';if(open&&!loaded)load();});return box;
}
async function refreshNodes(){
 const version=++generation,button=document.getElementById('refresh-nodes');button.disabled=true;directoryError.hidden=true;directoryNodes.replaceChildren(element('p','Loading members…','muted'));
 try{const data=await request('/api/topology');if(version!==generation)return;healthWindow=data.stale_after_ms||10000;document.getElementById('directory-count').textContent=data.members.length+' nodes';directoryNodes.replaceChildren(...data.members.map((m,i)=>card(m,i,version)));renderTopology(data);setParticipation(data);if(!data.members.length)directoryNodes.append(element('p','No members yet. Invite a node to join Union.','empty'));}
 catch(error){directoryError.textContent=error.message;directoryError.hidden=false;directoryNodes.replaceChildren();}finally{button.disabled=false;}
}
function setParticipation(data){for(const card of directoryNodes.querySelectorAll('.directory-node')){let label=card.querySelector('.participation-count');if(!label){label=element('span',undefined,'participation-count');card.querySelector('.node-facts').prepend(label);}label.textContent='Union jobs participated: '+(data.participation?.[card.dataset.identity]??'—')+' times';label.title='Distinct jobs in retained Union history; steps and retries do not increase this count.';}}
function renderTopology(data){DacpTopology.render(data,identity=>{
 const card=[...directoryNodes.querySelectorAll('.directory-node')].find(c=>c.dataset.identity===identity);if(!card)return;
 const heading=card.querySelector('.directory-heading');if(heading.getAttribute('aria-expanded')!=='true')heading.click();
 card.scrollIntoView({behavior:matchMedia('(prefers-reduced-motion: reduce)').matches?'instant':'smooth',block:'center'});
});}
document.getElementById('refresh-nodes').addEventListener('click',refreshNodes);refreshNodes();

setInterval(async()=>{
 if(document.getElementById('refresh-nodes').disabled||polling)return;
 const version=generation;polling=true;
 try{
  const data=await request('/api/topology'),cards=[...directoryNodes.querySelectorAll('.directory-node')];
  if(version!==generation)return;healthWindow=data.stale_after_ms||10000;
  if(cards.length!==data.members.length||cards.some((c,i)=>c.dataset.identity!==data.members[i].identity)){await refreshNodes();return;}
  cards.forEach((c,i)=>c.updateMember(data.members[i]));renderTopology(data);setParticipation(data);directoryError.hidden=true;
 }catch(error){if(version===generation){directoryError.textContent=error.message+' Showing the last observed node information.';directoryError.hidden=false;DacpTopology.stale();}}finally{polling=false;}
},5000);

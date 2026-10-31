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

async function api(path){const r=await fetch(path,{cache:'no-store'});if(!r.ok)throw new Error('Request failed ('+r.status+')');return r.json();}
function showError(e){$('error').textContent=e.message+'; current information is unavailable.';$('error').hidden=false;$('connection').textContent='Request failed';}
async function refresh(){if(busy)return;busy=true;try{
 const [info,page]=await Promise.all([api('/api/overview'),api('/api/history?offset='+offset+'&limit=50&source='+$('execution-source').value)]);
 $('address').textContent=info.flight_uri;$('summary').replaceChildren();
 for(const [value,label]of [[time(info.started_at_ms),'Started at'],[duration(info.uptime_ms),'Uptime'],[info.reserved_tasks+' / '+info.capacity,'Task capacity'],[info.executors.join(', '),'Executors']]){const stat=el('div',undefined,'stat');stat.append(el('strong',value),el('span',label));$('summary').append(stat);}
 const body=$('history');body.replaceChildren();for(const r of page.records){const row=el('tr');const button=el('a',r.name||'Unnamed job','job-link');button.title=r.record_id;button.href='/job?id='+encodeURIComponent(r.record_id);cell(row,button);const identifier=r.execution_id||r.record_id;const idCell=el('span',identifier.slice(0,8),'mono');idCell.title=identifier;cell(row,idCell);const source=el('span',r.execution_source==='union'?'Union':'Direct client','badge '+(r.execution_source==='union'?'busy':'good'));cell(row,source);cell(row,recordTypes[r.kind]||r.kind);cell(row,labels[r.state]||r.state);cell(row,time(r.created_at_ms));cell(row,duration(terminal.has(r.state)?r.elapsed_ms:Date.now()-r.created_at_ms));body.append(row);}
 if(!page.records.length){const row=el('tr');cell(row,'No execution records for this source').colSpan=7;body.append(row);}
 $('page').textContent=' '+(offset+1)+'–'+(offset+page.records.length)+' ';$('previous').disabled=offset===0;$('next').disabled=page.records.length<50;
 $('connection').textContent='Connected';$('error').hidden=true;
 }catch(e){showError(e);}finally{busy=false;}}

$('execution-source').addEventListener('change',()=>{offset=0;selected=null;previous.clear();refresh();});
$('refresh').addEventListener('click',()=>{refresh();});$('previous').addEventListener('click',()=>{offset=Math.max(0,offset-50);refresh();});$('next').addEventListener('click',()=>{offset+=50;refresh();});
refresh();setInterval(refresh,2000);

'use strict';
let previous=null,busy=false;
const byId=id=>document.getElementById(id);
function add(host,label,value){const card=document.createElement('div');card.className='stat';const strong=document.createElement('strong'),span=document.createElement('span');strong.textContent=value;span.textContent=label;card.append(strong,span);host.append(card);}
function size(value){if(value==null)return '—';for(const unit of ['B','kB','MB','GB']){if(value<1024||unit==='GB')return (unit==='B'?Math.round(value):value.toFixed(1))+' '+unit;value/=1024;}}
async function refresh(){
 if(busy)return;busy=true;
 try{
  const response=await fetch('/api/s3',{cache:'no-store',signal:AbortSignal.timeout(10000)});
  if(!response.ok)throw Error('HTTP '+response.status);
  const n=await response.json(),status=byId('s3-status'),statistics=byId('s3-statistics'),responses=byId('s3-responses');
  status.replaceChildren();statistics.replaceChildren();responses.replaceChildren();
  add(status,'Status',n.state);add(status,'Started at',n.started_at_ms?new Date(n.started_at_ms).toLocaleString():'—');
  add(status,'Uptime',n.enabled&&n.uptime_ms!=null?DacpDuration(n.uptime_ms):'—');
  add(status,'Active requests',n.active_requests??0);
  byId('s3-endpoint').textContent=n.enabled?'Endpoint: '+(n.endpoint||'Starting…'):'S3 is disabled on this node.';
  byId('s3-security').textContent=n.enabled?'HTTPS · read-only · '+(n.authentication||[]).join(' · '):'';
  let byteRate=null,requestRate=null;
  if(previous&&previous.started_at_ms===n.started_at_ms&&n.observed_at_ms>previous.observed_at_ms){
   const seconds=(n.observed_at_ms-previous.observed_at_ms)/1000;
   byteRate=Math.max(0,n.object_bytes_sent-previous.object_bytes_sent)/seconds;
   requestRate=Math.max(0,n.requests_total-previous.requests_total)/seconds;
  }
  for(const [label,value] of [['Requests',n.requests_total??0],['Succeeded',n.requests_succeeded??0],['Failed / cancelled',n.requests_failed??0],['Object bytes sent',size(n.object_bytes_sent)],['Transfer rate',byteRate==null?'—':size(byteRate)+'/s'],['Request rate',requestRate==null?'—':requestRate.toFixed(1)+'/s'],['Mean request time',n.mean_request_ms==null?'—':n.mean_request_ms.toFixed(1)+' ms'],['GET requests',n.methods?.GET??0],['HEAD requests',n.methods?.HEAD??0],['Other methods',n.methods?.OTHER??0]])add(statistics,label,value);
  for(const [code,count] of Object.entries(n.http_statuses||{})){const row=document.createElement('tr');for(const value of [code==='-1'?'No response headers':code,count]){const cell=document.createElement('td');cell.textContent=value;row.append(cell);}responses.append(row);}
  if(!responses.childElementCount){const row=document.createElement('tr'),cell=document.createElement('td');cell.colSpan=2;cell.textContent='No responses recorded';row.append(cell);responses.append(row);}
  previous=n;byId('s3-error').hidden=true;byId('s3-updated').textContent='Updated '+new Date().toLocaleTimeString();
 }catch(e){byId('s3-error').textContent='S3 statistics unavailable ('+e.message+'). Showing the previous snapshot.';byId('s3-error').hidden=false;byId('s3-updated').textContent='Unavailable';}
 finally{busy=false;}
}
byId('s3-refresh').addEventListener('click',refresh);refresh();setInterval(refresh,2000);

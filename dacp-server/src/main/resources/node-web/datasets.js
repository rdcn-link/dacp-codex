'use strict';
const rows=document.getElementById('catalog-rows'),status=document.getElementById('catalog-status'),error=document.getElementById('catalog-error'),refresh=document.getElementById('refresh-catalog'),search=document.getElementById('catalog-search');
let entries=[],loaded=false;
function text(tag,value,cls){const e=document.createElement(tag);e.textContent=value;if(cls)e.className=cls;return e;}
function render(){
 const query=search.value.trim().toLowerCase();
 const filtered=entries.filter(d=>[d.name,d.description,d.uri].some(v=>String(v).toLowerCase().includes(query)));
 rows.replaceChildren();
 for(const d of filtered){
  const row=document.createElement('tr'),description=document.createElement('td');
  description.append(text('pre',d.description,'dataset-description'));
  row.append(text('td',d.name,'mono'),description,text('td',d.uri,'mono'));rows.append(row);
 }
 if(!filtered.length){const row=document.createElement('tr'),cell=text('td',query?'No matching datasets':loaded?'No datasets are published by this node':'Catalog not loaded','empty');cell.colSpan=3;row.append(cell);rows.append(row);}
}
async function load(){
 refresh.disabled=true;error.hidden=true;rows.setAttribute('aria-busy','true');status.textContent='Loading local datasets…';
 try{
  const response=await fetch('/api/datasets',{cache:'no-store',signal:AbortSignal.timeout(15000)});
  if(!response.ok)throw Error('HTTP '+response.status);
  const data=await response.json();
  if(!Array.isArray(data.datasets))throw Error('Invalid catalog response');
  entries=data.datasets.sort((a,b)=>a.name.localeCompare(b.name));loaded=true;
  status.textContent=entries.length+' datasets · loaded '+new Date().toLocaleTimeString();render();
 }catch(e){
  status.textContent=loaded?'Showing the previous catalog':'Catalog unavailable';
  error.textContent='Unable to load the node catalog ('+e.message+'). Refresh to retry.';error.hidden=false;
  if(!loaded)render();
 }finally{refresh.disabled=false;rows.removeAttribute('aria-busy');}
}
search.addEventListener('input',render);refresh.addEventListener('click',load);load();

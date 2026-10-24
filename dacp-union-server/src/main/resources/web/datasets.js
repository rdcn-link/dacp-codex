'use strict';
const catalogRows=document.getElementById('catalog-rows'),catalogError=document.getElementById('catalog-error'),catalogStatus=document.getElementById('catalog-status');
let catalogEntries=[];
function textElement(tag,text,cls){const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;}
function renderCatalog(){
 const query=document.getElementById('catalog-search').value.trim().toLowerCase();
 const rows=catalogEntries.filter(d=>[d.name,d.description,d.uri].some(v=>v.toLowerCase().includes(query)));
 catalogRows.replaceChildren();
 for(const d of rows){const row=document.createElement('tr');row.append(textElement('td',d.name,'mono'));const description=document.createElement('td');description.append(textElement('pre',d.description,'dataset-description'));row.append(description,textElement('td',d.uri,'mono'));catalogRows.append(row);}
 if(!rows.length){const row=document.createElement('tr'),cell=textElement('td',query?'No matching datasets':'No datasets are available in this directory','empty');cell.colSpan=3;row.append(cell);catalogRows.append(row);}
}
async function refreshCatalog(){
 const button=document.getElementById('refresh-catalog');button.disabled=true;catalogError.hidden=true;catalogEntries=[];catalogRows.replaceChildren();catalogRows.setAttribute('aria-busy','true');catalogStatus.textContent='Reading live member catalogs…';
 try{
  const response=await fetch('/datasets/entries',{cache:'no-store'});if(!response.ok)throw new Error('Catalog request failed (HTTP '+response.status+'). Refresh to retry.');
  const data=await response.json();catalogEntries=data.datasets;renderCatalog();
  catalogStatus.textContent=catalogEntries.length+' datasets · '+new Date(data.observed_at_ms).toLocaleString('en-US');
  if(!data.complete){catalogError.textContent='Incomplete catalog: '+data.unavailable_sources+'  members could not provide a catalog. Only available datasets are shown; this directory is incomplete. Refresh to retry.';catalogError.hidden=false;}
 }catch(error){catalogStatus.textContent='Current catalog unavailable';catalogError.textContent=error.message;catalogError.hidden=false;}
 finally{button.disabled=false;catalogRows.removeAttribute('aria-busy');}
}
document.getElementById('refresh-catalog').addEventListener('click',refreshCatalog);document.getElementById('catalog-search').addEventListener('input',renderCatalog);refreshCatalog();

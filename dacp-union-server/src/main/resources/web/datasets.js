'use strict';
const catalogRows=document.getElementById('catalog-rows'),catalogError=document.getElementById('catalog-error'),catalogStatus=document.getElementById('catalog-status');
let catalogEntries=[],pendingSources=0;
function textElement(tag,text,cls){const n=document.createElement(tag);n.textContent=text;if(cls)n.className=cls;return n;}
function renderCatalog(){
 const query=document.getElementById('catalog-search').value.trim().toLowerCase();
 const rows=catalogEntries.filter(d=>[d.name,d.description,d.uri].some(v=>v.toLowerCase().includes(query)));
 catalogRows.replaceChildren();
 for(const d of rows){const row=document.createElement('tr');row.append(textElement('td',d.name,'mono'));const description=document.createElement('td');description.append(textElement('pre',d.description,'dataset-description'));row.append(description,textElement('td',d.uri,'mono'));catalogRows.append(row);}
 if(!rows.length){const row=document.createElement('tr'),cell=textElement('td',query?'No matching datasets':pendingSources?'Waiting for member catalogs…':'No datasets are available in this directory','empty');cell.colSpan=3;row.append(cell);catalogRows.append(row);}
}
async function catalogRequest(path){
 const response=await fetch(path,{cache:'no-store',signal:AbortSignal.timeout(15000)});
 if(!response.ok)throw new Error('HTTP '+response.status);
 return response.json();
}
async function refreshCatalog(){
 const button=document.getElementById('refresh-catalog');button.disabled=true;catalogError.hidden=true;catalogEntries=[];pendingSources=1;renderCatalog();catalogRows.setAttribute('aria-busy','true');catalogStatus.textContent='Reading live member catalogs…';
 try{
  const manifest=await catalogRequest('/datasets/sources');
  let completed=0;const failures=[];pendingSources=manifest.sources.length;
  const progress=()=>{
   catalogStatus.textContent=catalogEntries.length+' datasets · '+completed+'/'+manifest.sources.length+' members loaded'+(pendingSources?' · loading…':'');
   catalogError.hidden=failures.length===0;
   catalogError.textContent='Incomplete catalog. Unavailable members: '+failures.join(', ')+'. Refresh to retry.';
   renderCatalog();
  };
  progress();
  await Promise.allSettled(manifest.sources.map(async source=>{
   try{
    const data=await catalogRequest('/datasets/sources/'+encodeURIComponent(source.identity));
    catalogEntries.push(...data.datasets);catalogEntries.sort((a,b)=>a.name.localeCompare(b.name));
   }catch(error){failures.push(source.url+' ('+error.message+')');}
   finally{completed++;pendingSources--;progress();}
  }));
 }catch(error){catalogStatus.textContent='Current catalog unavailable';catalogError.textContent=error.message;catalogError.hidden=false;}
 finally{pendingSources=0;renderCatalog();button.disabled=false;catalogRows.removeAttribute('aria-busy');}
}
document.getElementById('refresh-catalog').addEventListener('click',refreshCatalog);document.getElementById('catalog-search').addEventListener('input',renderCatalog);refreshCatalog();

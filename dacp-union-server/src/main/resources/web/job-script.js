'use strict';
window.DacpJobScript={format:script=>script||'No job script was saved for this historical record'};

// One shared YAML toolbar for node and Union dashboards, including sub-job views.
function installYamlCopy(){
 for(const script of document.querySelectorAll('pre.job-script')){
  if(script.parentElement.classList.contains('yaml-block'))continue;
  const block=document.createElement('div');block.className='yaml-block';
  const toolbar=document.createElement('div');toolbar.className='yaml-toolbar';
  const status=document.createElement('span');status.className='yaml-copy-status';status.setAttribute('role','status');status.setAttribute('aria-live','polite');
  const button=document.createElement('button');button.type='button';button.className='yaml-copy';button.title='Copy YAML';button.setAttribute('aria-label','Copy YAML');
  button.innerHTML='<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.7" aria-hidden="true"><rect x="8" y="8" width="12" height="13" rx="2"/><path d="M16 8V5a2 2 0 0 0-2-2H5a2 2 0 0 0-2 2v9a2 2 0 0 0 2 2h3"/></svg><span>Copy</span>';
  async function copy(text){
   if(window.isSecureContext && navigator.clipboard?.writeText){
    try{await navigator.clipboard.writeText(text);return;}catch(error){/* Use the HTTP-compatible fallback below. */}
   }
   const active=document.activeElement,selection=window.getSelection();
   const ranges=selection?Array.from({length:selection.rangeCount},(_,i)=>selection.getRangeAt(i).cloneRange()):[];
   const input=document.createElement('textarea');input.value=text;input.readOnly=true;input.className='yaml-copy-buffer';document.body.append(input);
   try{input.focus({preventScroll:true});input.select();if(!document.execCommand('copy'))throw new Error('Copy unavailable');}
   finally{input.remove();if(selection){selection.removeAllRanges();for(const range of ranges)selection.addRange(range);}active?.focus({preventScroll:true});}
  }
  button.addEventListener('click',async()=>{
   button.disabled=true;status.textContent='';
   try{await copy(script.textContent);status.textContent='Copied';}
   catch(error){status.textContent='Copy failed. Select the YAML and copy manually.';}
   finally{button.disabled=false;}
  });
  // A different sub-job or a manual refresh replaces the text in the same element.
  new MutationObserver(()=>{status.textContent='';}).observe(script,{childList:true,characterData:true,subtree:true});
  script.before(block);block.append(toolbar,script);toolbar.append(status,button);
 }
}
if(document.readyState==='loading')document.addEventListener('DOMContentLoaded',installYamlCopy,{once:true});else installYamlCopy();

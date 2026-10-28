'use strict';
const $=id=>document.getElementById(id),colors=['#356ce1','#c45b16','#168464','#9252cb','#be3769'];
let config,operators=[],nodes=[],connected=null,job={name:'Untitled job',tasks:[],outputs:{}},positions={},selected=null,pendingPort=null,scale=1,report=null,revision=0,proposal=null;
let nodeAreas={},laneBounds=[],selectedNodeRegion=null,selectedSteps=new Set(),suppressCanvasClick=false,graphInteraction=false;
let inspectedSelection=null,parameterEditor=null,selectedEdge=null,outletSelected=false,outletPosition=null;
let undo=[],redo=[],chatHistory=[],yamlEdited=false,yamlTimer=null,docSequence=0;
let automaticReadViews=new Set();
let inferenceTimer=null,inferenceRunning=false,inferenceSequence=0,inferencePending=false,inferenceState='Not inferred',inferenceSignature=null,inferenceDraftKey=null;
const clone=x=>JSON.parse(JSON.stringify(x));
function el(tag,text,cls){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;if(cls)e.className=cls;return e;}
const logEntries=[],logLimit=1000;
let logSequence=0;
function logLevel(level){return ['error','fatal'].includes(String(level).toLowerCase())?'error':['warn','warning'].includes(String(level).toLowerCase())?'warning':'info';}
function setStatus(text,level){$('notice').textContent=text;$('notice').classList.toggle('error',level==='error');$('notice').classList.toggle('warning',level==='warning');$('notice').title=text;}
function renderLogs(resetView=false){
 const output=$('log-output'),filter=$('log-filter').value,previousTop=output.scrollTop;
 const anchor=!resetView&&!$('log-follow').checked?[...output.children].find(row=>row.offsetTop+row.offsetHeight>previousTop):null;
 const anchorId=anchor?.dataset.logId,anchorOffset=anchor?anchor.offsetTop-previousTop:0;
 output.replaceChildren();
 for(const entry of [...logEntries].reverse()){
  if(filter!=='all'&&entry.level!==filter)continue;
  const row=el('div',undefined,'log-entry log-'+entry.level);row.dataset.logId=entry.id;
  row.append(el('time',entry.time),el('span',entry.level.toUpperCase(),'log-level'),el('span',entry.source,'log-source'),el('span',entry.text,'log-message'));output.append(row);
 }
 $('log-count').textContent=logEntries.length;
 const restored=anchorId?output.querySelector('[data-log-id="'+anchorId+'"]'):null;
 output.scrollTop=$('log-follow').checked||resetView?0:restored?restored.offsetTop-anchorOffset:previousTop;
}
function logMessage(text,level='info',source='Designer'){level=logLevel(level);text=String(text);logEntries.push({id:++logSequence,text,level,source,time:new Date().toLocaleTimeString([], {hour12:false})});if(logEntries.length>logLimit)logEntries.shift();setStatus(text,level);renderLogs();}
function notice(text,error=false){logMessage(text,error?'error':'info');}
async function api(path,body){const started=performance.now();logMessage('Request started: '+path,'info','API');try{const r=await fetch('/api/'+path,body===undefined?{}:{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});const n=await r.json();if(!r.ok)throw Error(n.error||'Request failed (HTTP '+r.status+')');logMessage('Request completed: '+path+' ('+Math.round(performance.now()-started)+' ms)','info','API');return n;}catch(e){logMessage(path+': '+e.message,'error','API');e.designerLogged=true;throw e;}}
function guard(fn){return async(...args)=>{try{return await fn(...args);}catch(e){if(!e.designerLogged)notice(e.message,true);}};}
window.addEventListener('error',e=>logMessage(e.message||'Unexpected browser error','error','Browser'));
window.addEventListener('unhandledrejection',e=>{if(!e.reason?.designerLogged)logMessage(e.reason?.message||String(e.reason),'error','Browser');});
const jobYamlEditor=YamlEditors.create($('yaml'),{label:'Job YAML',onChange:()=>{yamlEdited=true;inferenceSequence++;report=null;inferenceState='Apply YAML to infer';refreshPortSchemas();}});
const variablesEditor=YamlEditors.create($('variables'),{label:'Variables YAML',text:'{}',onChange:()=>{invalidate();persist();}});
const exportYamlEditor=YamlEditors.create($('export-preview'),{label:'Job YAML preview',readOnly:true});
function nodeColor(id){const index=nodes.findIndex(n=>n.id===id);return colors[Math.max(0,index)%colors.length];}
function nodeLabel(id){const n=nodes.find(n=>n.id===id);if(!n?.address)return 'Node unavailable';try{return new URL(n.address).host;}catch{return n.address;}}
function viewport(){return{zoom:scale,scroll_left:$('canvas').scrollLeft,scroll_top:$('canvas').scrollTop};}
function designLayout(){return{version:1,generator:clone(config.generator),exported_at:new Date().toISOString(),positions:Object.fromEntries(job.tasks.map(t=>[t.id,clone(point(t))])),node_areas:clone(nodeAreas),outlet:clone(outletPosition||defaultOutletPosition()),viewport:viewport()};}
function executionDocument(){const doc=clone(job);for(const t of doc.tasks)if(automaticReadViews.has(t.id)&&t.operator==='read_item@1.0')delete t.params.view;return doc;}
function payload(includeLayout=true){const doc=executionDocument();return{job_yaml:JSON.stringify(includeLayout?{...doc,designer:designLayout()}:doc),variables_yaml:variablesEditor.getText()};}
function layoutChanged(){persist();clearTimeout(yamlTimer);yamlTimer=setTimeout(()=>guard(syncYaml)(),250);}
function restoreViewport(view){scale=view?.zoom||1;$('world').style.transform=`scale(${scale})`;$('zoom').value=Math.round(scale*100)+'%';$('canvas').scrollLeft=view?.scroll_left||0;$('canvas').scrollTop=view?.scroll_top||0;}

function snapshot(){return{job:clone(job),positions:clone(positions),nodeAreas:clone(nodeAreas),outletPosition:clone(outletPosition),viewport:viewport(),automaticReadViews:[...automaticReadViews],variables:variablesEditor.getText()};}
function recordSnapshot(state){undo.push(state);if(undo.length>60)undo.shift();redo=[];}
function remember(){recordSnapshot(snapshot());}
function invalidate(){revision++;proposal=null;$('apply-chat').disabled=true;$('discard-chat').disabled=true;scheduleInference();}
function persist(){try{localStorage.setItem('dacp-job-designer',JSON.stringify(snapshot()));}catch(e){notice('Browser draft storage is unavailable. Export YAML to keep your work.');}}
function changed(){invalidate();render();persist();clearTimeout(yamlTimer);yamlTimer=setTimeout(()=>guard(syncYaml)(),200);}
async function syncYaml(){const version=++docSequence;const response=await api('document',payload());if(version===docSequence&&!yamlEdited){jobYamlEditor.setText(response.job_yaml);}return response;}
function restore(s){selectedNodeRegion=null;cancelConnection();job=clone(s.job);automaticReadViews=new Set(s.automaticReadViews||[]);positions=clone(s.positions);nodeAreas=clone(s.nodeAreas||{});outletPosition=s.outletPosition?clone(s.outletPosition):null;outletSelected=false;variablesEditor.setText(s.variables);selected=null;selectedSteps.clear();selectedEdge=null;yamlEdited=false;changed();restoreViewport(s.viewport);}
const readParameters={type:"object",properties:{uri:{type:"string",description:"Public dataset Item URI",minLength:1},view:{type:"string",description:"Defaults to records when the Item supports it, otherwise bytes. Choose a value to override.",enum:["bytes","records"],default:"records"}},required:["uri"],additionalProperties:false};
const readIcon={path:'M5 6h14v5H5zM12 11v9m-4-4 4 4 4-4',color:'#2378cb'};
function operatorIcon(icon){const svg=document.createElementNS('http://www.w3.org/2000/svg','svg');svg.setAttribute('viewBox','0 0 24 24');svg.setAttribute('aria-hidden','true');const path=document.createElementNS(svg.namespaceURI,'path');path.setAttribute('d',icon.path);svg.style.color=icon.color;svg.append(path);return svg;}
function parameterSchema(t){return t.operator==='read_item@1.0'?readParameters:manifest(t)?.parameters;}
function manifest(task){const [name,version]=task.operator.split('@');return operators.find(o=>o.name===name&&o.version===version);}
const binding=v=>{const [task,port='result']=String(v).split(':');return{task,port};};
function outputPorts(t){if(report?.tasks?.[t.id]&&!report.tasks[t.id].diagnostics?.some(d=>d.severity==='error'))return Object.keys(report.tasks[t.id].outputs||{});const names=new Set(['result']);for(const x of job.tasks)for(const v of Object.values(x.inputs||{})){const b=binding(v);if(b.task===t.id)names.add(b.port);}for(const v of Object.values(job.outputs)){const b=binding(v);if(b.task===t.id)names.add(b.port);}return [...names];}
function inputPorts(t){return [...new Set([...Object.keys(manifest(t)?.inputs||{}),...Object.keys(t.inputs||{})])];}
function color(t){const ids=[...new Set(job.tasks.map(t=>t.node||''))];return colors[Math.max(0,ids.indexOf(t.node||''))%colors.length];}
function point(t){return positions[t.id]||(positions[t.id]={x:70+job.tasks.indexOf(t)*310,y:85});}
function addOperator(op,x,y){const before=snapshot();let i=1;while(job.tasks.some(t=>t.id==='step_'+i))i++;const id='step_'+i;const t={id,title:op.name,operator:op.name+'@'+op.version,inputs:{},params:{}};const area=nodeAt(x,y);if(area)t.node=area;if(op.name==='read_item'){t.params={uri:'$INPUT_URI',view:'records'};automaticReadViews.add(id);}job.tasks.push(t);positions[id]={x:Math.max(30,x),y:Math.max(55,y)};selected=id;selectedSteps=new Set([id]);outletSelected=false;render();avoidSteps(dragLayout(),new Set([id]),false);recordSnapshot(before);changed();}

const libraryExpanded=new Map();
let operatorTipTimer;
function hideOperatorTip(){clearTimeout(operatorTipTimer);document.querySelectorAll('[aria-describedby=operator-tooltip]').forEach(e=>e.removeAttribute('aria-describedby'));$('operator-tooltip')?.remove();}
function showOperatorTip(card,o,group){hideOperatorTip();const tip=el('div',undefined,'operator-tooltip');tip.id='operator-tooltip';tip.setAttribute('role','tooltip');tip.append(el('strong',o.name),el('small','Version '+o.version+' · '+group.category),el('p',o.description||'No description supplied.'));const ports=Object.entries(o.inputs||{}).map(([name,p])=>name+' ('+(p.representations||[]).join(' / ')+')');tip.append(el('div','Inputs: '+(ports.join(', ')||'None — source operator'),'tooltip-detail'));const params=Object.entries(o.parameters?.properties||{}).map(([name,p])=>name+': '+(Array.isArray(p.type)?p.type.join(' / '):p.type||'any')+((o.parameters?.required||[]).includes(name)?' *':''));tip.append(el('div','Parameters: '+(params.join(', ')||'None'),'tooltip-detail'),el('div','Executor: '+(o.execution?.executor||'Built-in'),'tooltip-detail'),el('small','Drag onto canvas or double-click to add.'));document.body.append(tip);card.setAttribute('aria-describedby',tip.id);const r=card.getBoundingClientRect(),height=tip.getBoundingClientRect().height;tip.style.left=Math.min(r.right+12,Math.max(8,innerWidth-tip.offsetWidth-8))+'px';tip.style.top=Math.max(8,Math.min(r.top,innerHeight-height-8))+'px';}
function renderLibrary(){hideOperatorTip();const query=$('search').value.trim().toLowerCase(),host=$('operators');host.replaceChildren();const source={name:'read_item',version:'1.0',description:'Read an Item as a byte or record stream.',inputs:{},parameters:readParameters,category:'Data sources',icon:readIcon};const libraryGroups=[...new Set([source,...operators].map(o=>o.category))].sort().map(category=>({category,id:category}));let count=0;
 for(const group of libraryGroups){const entries=[source,...operators].filter(o=>o.category===group.category&&(o.name+' '+(o.description||'')+' '+group.category).toLowerCase().includes(query));if(!entries.length)continue;count+=entries.length;const section=el('details',undefined,'operator-category');section.dataset.category=group.id;section.open=query?true:libraryExpanded.get(group.id)!==false;const heading=el('summary');heading.append(el('span',group.category),el('small',String(entries.length)));section.append(heading);section.addEventListener('toggle',()=>{if(!query)libraryExpanded.set(group.id,section.open);hideOperatorTip();});const grid=el('div',undefined,'operator-grid');
 for(const o of entries){const card=el('button',undefined,'operator');card.type='button';card.draggable=true;card.dataset.operator=o.name+'@'+o.version;card.setAttribute('aria-label',o.name+' @ '+o.version);const svg=operatorIcon(o.icon);card.append(svg,el('span',o.name,'operator-name'),el('small','v'+o.version));card.addEventListener('mouseenter',()=>{operatorTipTimer=setTimeout(()=>showOperatorTip(card,o,group),250);});card.addEventListener('mouseleave',hideOperatorTip);card.addEventListener('focus',()=>showOperatorTip(card,o,group));card.addEventListener('blur',hideOperatorTip);card.addEventListener('dragstart',e=>{hideOperatorTip();e.dataTransfer.setData('application/dacp-operator',card.dataset.operator);});const add=()=>{hideOperatorTip();addOperator(o,job.tasks.length?Math.max(...job.tasks.map(t=>point(t).x))+310:65,85);};card.addEventListener('dblclick',add);card.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();add();}});grid.append(card);}section.append(grid);host.append(section);
 }if(!count)host.append(el('p','No matching operators.','muted'));}


function schemaStatus(box,kind,text){
 box.classList.add('schema-status','schema-'+kind);
 const icon=el('span',kind==='error'?'!':kind==='unknown'?'?':'…','schema-status-icon');icon.setAttribute('aria-hidden','true');
 box.append(icon,el('span',text));
}
function renderOutputPorts(t,outs){
 outs.replaceChildren();
 for(const port of outputPorts(t)){
  const b=el('button',port);b.dataset.output=t.id+':'+port;b.classList.toggle('active',pendingPort===b.dataset.output);b.title='Connect this output to an input.';
  b.addEventListener('click',e=>{e.stopPropagation();selectedEdge=null;pendingPort=pendingPort===b.dataset.output?null:b.dataset.output;pointerPosition={x:e.clientX,y:e.clientY};notice(pendingPort?'Click an input port to connect '+pendingPort+'. Right-click or press Esc to cancel.':'Connection cancelled');render();});outs.append(b);
  const d=report?.tasks?.[t.id]?.outputs?.[port],box=el('div',undefined,'port-schema');box.dataset.schemaPort=t.id+':'+port;
  if(!d){const errors=report?.tasks?.[t.id]?.diagnostics?.filter(x=>x.severity==='error')||[];if(errors.length||inferenceState.startsWith('Inference failed'))schemaStatus(box,'error','Inference error');else if(inferenceRunning||inferencePending)schemaStatus(box,'pending','Inferring…');else schemaStatus(box,'unknown','Schema unknown');}else{
   let schema=d.schema_json||d.schema;if(typeof schema==='string'){try{schema=JSON.parse(schema);}catch{schema=null;}}
   if(schema?.fields){if(schema.fields.length){const table=el('table',undefined,'port-schema-table');table.setAttribute('aria-label','Output fields and types');const body=el('tbody');for(const f of schema.fields){const row=el('tr'),name=el('th',f.name),type=el('td',SchemaTypes.fieldType(f));name.scope='row';name.title=f.name;type.title=SchemaTypes.fieldType(f);row.append(name,type);body.append(row);}table.append(body);box.append(table);}else box.append(el('span','Empty schema'));}
   else if(d.structure){box.append(el('code',JSON.stringify(d.structure),'port-schema-structure'));}
   else if((d.representations||[]).includes('records')||d.certainty==='unknown')schemaStatus(box,'unknown','Schema unknown');
   else box.append(el('span',(d.formats||[]).join(' / ')||'Byte stream'));

  }
  outs.append(box);
 }
}
function refreshPortSchemas(){if(graphInteraction||job.tasks.some(t=>![...$('cards').children].some(c=>c.dataset.task===t.id)))return;for(const t of job.tasks){const card=[...$('cards').children].find(c=>c.dataset.task===t.id);if(card)renderOutputPorts(t,card.querySelector('.ports .out'));}drawLanes();keepOutletOutsideRegions(laneBounds);drawEdges();refreshStepOutputs();}
function inferencePayload(){const n=payload(false),draft=JSON.parse(n.job_yaml);if(selected&&parameterEditor&&!$('inspector').hidden){const t=draft.tasks.find(t=>t.id===selected);if(t)t.params=parameterEditor.read();}for(const t of draft.tasks)if(automaticReadViews.has(t.id)&&t.operator==='read_item@1.0')delete t.params.view;n.job_yaml=JSON.stringify(draft);return n;}
function applyReadViewDefaults(data){for(const t of job.tasks){if(!automaticReadViews.has(t.id)||t.operator!=='read_item@1.0')continue;const repr=data.tasks?.[t.id]?.outputs?.result?.representations?.[0];if(!['records','bytes'].includes(repr))continue;t.params.view=repr;if(selected===t.id&&parameterEditor&&!$('inspector').hidden)parameterEditor.setValue('view',repr);}}

// Only operator contracts, parameters, input bindings and variable values affect inference.
function inferenceKey(request){const draft=JSON.parse(request.job_yaml);return JSON.stringify({tasks:draft.tasks.map(t=>({id:t.id,operator:t.operator,inputs:t.inputs||{},params:t.params||{}})),variables:request.variables_yaml});}
function scheduleInference(){let key;try{key=inferenceKey(inferencePayload());}catch{key=null;}if(key&&key===inferenceDraftKey)return;inferenceDraftKey=key;clearTimeout(inferenceTimer);inferenceSequence++;inferencePending=true;inferenceSignature=null;report=null;inferenceState='Waiting for inference…';$('output-preview-status').textContent='Output schemas will update automatically.';$('step-output-preview').replaceChildren();if($('cards'))refreshPortSchemas();inferenceTimer=setTimeout(()=>runInference(),650);}
async function inferStepStream(request,seq){
 const started=performance.now();logMessage('Structure inference started','info','Schema');
 const response=await fetch('/api/infer_stream',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(request)});
 if(!response.ok){const error=await response.json();throw Error(error.error||'Inference request failed');}
 const reader=response.body.getReader(),decoder=new TextDecoder();let pending='',complete=false;
 function accept(line){if(!line.trim())return;const event=JSON.parse(line);if(seq!==inferenceSequence)return;
  if(event.event==='error')throw Error(event.message);
  if(event.event==='step'){if(!report)report={tasks:{},outputs:{}};report.tasks[event.step]=event.result;applyReadViewDefaults(report);refreshPortSchemas();logMessage('Structure ready: '+event.step,'info','Schema');}
  if(event.event==='complete'){report=event.report;complete=true;}
 }
 try{while(true){const {done,value}=await reader.read();if(seq!==inferenceSequence){await reader.cancel();return null;}pending+=decoder.decode(value||new Uint8Array(),{stream:!done});let end;while((end=pending.indexOf('\n'))>=0){accept(pending.slice(0,end));pending=pending.slice(end+1);}if(done){accept(pending);break;}}if(!complete)throw Error('Inference stream ended before completion');logMessage('Structure inference completed ('+Math.round(performance.now()-started)+' ms)','info','Schema');return report;}finally{reader.releaseLock();}
}
async function runInference(force=false){
 if(inferenceRunning){inferencePending=true;return;}
 if(connected!==true||!job.tasks.length||yamlEdited)return;
 let request;try{request=inferencePayload();}catch(e){inferenceState='Complete parameters to infer';$('output-preview-status').textContent=e.message;logMessage(e.message,'warning','Schema');refreshPortSchemas();return;}
 const signature=inferenceKey(request);if(!force&&signature===inferenceSignature)return;
 inferencePending=false;inferenceRunning=true;const seq=inferenceSequence;inferenceState='Inferring…';$('output-preview-status').textContent='Inferring output schemas…';refreshPortSchemas();
 try{const result=await inferStepStream(request,seq);if(seq!==inferenceSequence){logMessage('Discarded outdated schema response; newer parameters are pending.','info','Schema');return;}report=result;inferenceSignature=signature;inferenceState='Schema unknown';applyReadViewDefaults(result);showSchema(result);refreshPortSchemas();
 // Publish schemas before reading any sample rows. A slow sample must not hide valid schemas.
 if(seq===inferenceSequence){try{const sampled=await api('infer',{...request,preview:{max_rows:10,max_bytes:16384}});if(seq===inferenceSequence){report=sampled;refreshPortSchemas();}}catch(e){if(seq===inferenceSequence)logMessage('Output samples unavailable: '+e.message,'warning','Schema');}}
 }
 catch(e){if(seq===inferenceSequence){inferenceState='Inference failed: '+e.message;$('output-preview-status').textContent=e.message;refreshPortSchemas();}}
 finally{inferenceRunning=false;refreshStepOutputs();if(inferencePending){clearTimeout(inferenceTimer);inferenceTimer=setTimeout(()=>runInference(),650);}}
}

function render(){
 if(graphInteraction)return;
 $('job-name').value=job.name;$('empty').hidden=job.tasks.length>0;$('undo').disabled=!undo.length;$('redo').disabled=!redo.length;
 const cards=$('cards');cards.replaceChildren();
 for(const t of job.tasks){const p=point(t),card=el('article',undefined,'card'+(t.id===selected||selectedSteps.has(t.id)?' selected':''));card.dataset.task=t.id;card.style.left=p.x+'px';card.style.top=p.y+'px';card.style.setProperty('--accent',color(t));const head=el('div',undefined,'card-head');const icon=operatorIcon(t.operator==='read_item@1.0'?readIcon:manifest(t)?.icon||{path:'M4 5h16v14H4zM8 9h8M8 13h5',color:'#77879c'});icon.classList.add('step-icon');const text=el('div',undefined,'step-heading');text.append(el('strong',t.title||t.id),el('small',t.id+' · '+t.operator));head.append(icon,text);head.addEventListener('pointerdown',e=>startDrag(e,t,card));card.append(head);const ports=el('div',undefined,'ports'),ins=el('div'),outs=el('div',undefined,'out');
 for(const port of inputPorts(t)){const b=el('button',port);b.dataset.input=t.id+':'+port;b.title=(t.inputs||{})[port]||'Unconnected input';b.addEventListener('click',e=>{e.stopPropagation();if(pendingPort)connectPort(t,port);else{selectedSteps=new Set([t.id]);outletSelected=false;selected=t.id;render();}});ins.append(b);}
 renderOutputPorts(t,outs);

 ports.append(ins,outs);card.append(ports);if(t.node)card.append(el('div',nodeLabel(t.node),'placement'));card.addEventListener('click',e=>{if(card.dataset.dragged==='true'||card.dataset.handled==='true'){e.stopPropagation();return;}selectStep(t,e.shiftKey);});card.addEventListener('dblclick',e=>{if(e.target.closest('.ports'))return;e.stopPropagation();selectedSteps=new Set([t.id]);selected=t.id;selectedEdge=null;selectedNodeRegion=null;outletSelected=false;setInspector(true);render();$('canvas').focus({preventScroll:true});});cards.append(card);}
 if(selectedEdge&&(selectedEdge.output?!Object.hasOwn(job.outputs,selectedEdge.output):!job.tasks.some(t=>t.id===selectedEdge.task&&t.inputs?.[selectedEdge.port])))selectedEdge=null;if(selectedEdge){outletSelected=false;selectedSteps.clear();}renderOutlet();$('canvas').classList.toggle('edge-selected',!!selectedEdge);drawLanes();keepOutletOutsideRegions(laneBounds);drawEdges();inspect();
}
function selectStep(t,append=false){selectedNodeRegion=null;if(append){if(selectedSteps.has(t.id))selectedSteps.delete(t.id);else selectedSteps.add(t.id);}else{selectedSteps=new Set([t.id]);outletSelected=false;}selectedEdge=null;selected=selectedSteps.size===1&&!outletSelected?[...selectedSteps][0]:null;document.querySelectorAll('.card').forEach(card=>card.classList.toggle('selected',selectedSteps.has(card.dataset.task)));renderOutlet();$('canvas').classList.remove('edge-selected');drawEdges();inspect();$('canvas').focus({preventScroll:true});}
// Steps and whole node regions push neighboring elements aside.
function dragLayout(){return job.tasks.map(t=>{const card=[...$('cards').children].find(c=>c.dataset.task===t.id);return{id:t.id,...clone(point(t)),width:card.offsetWidth,height:card.offsetHeight,card};});}
function overlaps(a,b){return a.x<b.x+b.width&&a.x+a.width>b.x&&a.y<b.y+b.height&&a.y+a.height>b.y;}
function outletRect(){return{...outletPosition,width:$('outlet').offsetWidth,height:$('outlet').offsetHeight};}
// Rebuild each preview from the drag-start layout, so moving back never leaves displaced neighbors behind.
function avoidRectangles(rects,direction=1){
 const queue=rects.filter(r=>r.fixed),gap=18;
 // All cascades follow one vertical direction; the dragged group stays fixed.
 const moveAside=(r,obstacle)=>{r.y=direction<0?obstacle.y-r.height-gap:obstacle.y+obstacle.height+gap;};
 while(queue.length){const obstacle=queue.shift();for(const r of rects){if(r===obstacle||r.fixed||!overlaps(r,obstacle))continue;moveAside(r,obstacle);let blocker;while((blocker=rects.find(other=>other.fixed&&overlaps(r,other))))moveAside(r,blocker);queue.push(r);}}
 // Keep an upward cascade visible when it reaches the canvas origin.
 if(direction<0){const offset=Math.max(0,30-Math.min(...rects.map(r=>r.y)));if(offset)for(const r of rects)r.y+=offset;}
}
function avoidSteps(layout,fixedIds,movingOutlet){
 const rects=layout.map(r=>({...r,...positions[r.id],fixed:fixedIds.has(r.id)}));
 rects.push({...outletRect(),id:null,fixed:movingOutlet});
 avoidRectangles(rects);
 for(const r of rects){if(r.id){positions[r.id]={x:r.x,y:r.y};r.card.style.left=r.x+'px';r.card.style.top=r.y+'px';}else outletPosition={x:r.x,y:r.y};}
 keepOutletOutsideRegions([...laneBounds,...nodeRegionBounds()]);
}
function restoreDragLayout(state){positions=clone(state.positions);nodeAreas=clone(state.nodeAreas);outletPosition=clone(state.outletPosition);inspectedSelection=outletSelected&&!selectedSteps.size?$('outlet'):selected;render();}
function worldExtent(rects){const right=Math.max(6000,...rects.map(r=>r.x+r.width+200)),bottom=Math.max(4000,...rects.map(r=>r.y+r.height+200));$('world').style.width=right+'px';$('world').style.height=bottom+'px';$('edges').setAttribute('width',right);$('edges').setAttribute('height',bottom);}
function startDrag(e,t,card){if(e.button!==0||pendingPort)return;graphInteraction=true;selectedNodeRegion=null;
 const already=t?selectedSteps.has(t.id):outletSelected;
 if(!already&&!e.shiftKey){selectedSteps.clear();outletSelected=false;}
 if(t)selectedSteps.add(t.id);else outletSelected=true;
 selectedEdge=null;selected=selectedSteps.size===1&&!outletSelected?[...selectedSteps][0]:null;
 const layout=dragLayout(),before=snapshot(),starts=new Map(job.tasks.filter(x=>selectedSteps.has(x.id)).map(x=>[x.id,clone(point(x))])),outStart=outletSelected?clone(outletPosition):null,start={x:e.clientX,y:e.clientY};let moved=false;
 card.setPointerCapture(e.pointerId);
 function move(ev){if(!moved&&Math.hypot(ev.clientX-start.x,ev.clientY-start.y)>3){moved=true;card.dataset.dragged='true';}if(!moved)return;let dx=(ev.clientX-start.x)/scale,dy=(ev.clientY-start.y)/scale;const points=[...starts.values(),...(outStart?[outStart]:[])];dx=Math.max(dx,25-Math.min(...points.map(p=>p.x)));dy=Math.max(dy,50-Math.min(...points.map(p=>p.y)));
 positions=clone(before.positions);outletPosition=clone(before.outletPosition);for(const r of layout){r.card.style.left=positions[r.id].x+'px';r.card.style.top=positions[r.id].y+'px';}renderOutlet();
 for(const [id,p]of starts){positions[id]={x:p.x+dx,y:p.y+dy};const node=[...$('cards').children].find(c=>c.dataset.task===id);if(node){node.style.left=positions[id].x+'px';node.style.top=positions[id].y+'px';node.classList.add('selected');}}
 if(outStart){outletPosition={x:outStart.x+dx,y:outStart.y+dy};renderOutlet();}avoidSteps(layout,new Set(starts.keys()),!!outStart);worldExtent([...layout.map(r=>({...r,...positions[r.id]})),outletRect()]);drawEdges();}
 function end(ev){graphInteraction=false;card.removeEventListener('pointermove',move);card.removeEventListener('pointerup',end);card.removeEventListener('pointercancel',end);card.dataset.handled='true';setTimeout(()=>delete card.dataset.handled,0);
 if(ev.type==='pointercancel'){if(moved)restoreDragLayout(before);return;}
 suppressCanvasClick=true;setTimeout(()=>suppressCanvasClick=false,0);
 if(moved){recordSnapshot(before);for(const x of job.tasks.filter(x=>starts.has(x.id))){const p=point(x),id=nodeAt(p.x+30,p.y+15);if(id)x.node=id;else delete x.node;}inspectedSelection=outletSelected&&!selectedSteps.size?$('outlet'):selected;changed();}
 else if(t){if(e.shiftKey&&!already)selectedSteps.delete(t.id);selectStep(t,e.shiftKey);}else selectOutlet(e.shiftKey);
 $('canvas').focus({preventScroll:true});}
 card.addEventListener('pointermove',move);card.addEventListener('pointerup',end);card.addEventListener('pointercancel',end);
}
let pointerPosition=null,previewFrame=null;
function cancelConnection(){if(!pendingPort)return;pendingPort=null;pointerPosition=null;document.querySelectorAll('[data-output].active,.connection-target').forEach(e=>e.classList.remove('active','connection-target'));drawPreview();notice('Connection cancelled.');}
document.addEventListener('pointermove',e=>{pointerPosition={x:e.clientX,y:e.clientY};if(pendingPort&&previewFrame===null)previewFrame=requestAnimationFrame(()=>{previewFrame=null;drawPreview();});});
document.addEventListener('contextmenu',e=>{if(pendingPort){e.preventDefault();cancelConnection();}});
$('canvas').addEventListener('scroll',()=>{if(pendingPort)drawPreview();});
function drawPreview(){
 document.querySelectorAll('.connection-target').forEach(e=>e.classList.remove('connection-target'));
 if(!pendingPort){$('connection-preview')?.remove();return;}
 const source=binding(pendingPort),a=portPoint(source.task,source.port,true);if(!a||!pointerPosition){$('connection-preview')?.remove();return;}
 const world=$('world').getBoundingClientRect();let z={x:(pointerPosition.x-world.left)/scale,y:(pointerPosition.y-world.top)/scale};
 const target=document.elementFromPoint(pointerPosition.x,pointerPosition.y)?.closest('[data-input],#outlet');
 if(target?.id==='outlet'){target.classList.add('connection-target');z=outletPoint();}
 else if(target){const b=binding(target.dataset.input);if(b.task!==source.task){target.classList.add('connection-target');z=portPoint(b.task,b.port,false)||z;}}
 let path=$('connection-preview');if(!path){path=document.createElementNS('http://www.w3.org/2000/svg','path');path.id='connection-preview';path.setAttribute('aria-hidden','true');$('edges').append(path);}
 const d=Math.max(60,Math.abs(z.x-a.x)*.45);path.setAttribute('d',`M${a.x},${a.y} C${a.x+d},${a.y} ${z.x-d},${z.y} ${z.x},${z.y}`);path.classList.toggle('snapped',!!target&&target.classList.contains('connection-target'));
}
function portPoint(task,port,output){const card=[...$('cards').children].find(c=>c.dataset.task===task);if(!card)return null;const button=[...card.querySelectorAll(output?'[data-output]':'[data-input]')].find(b=>(output?b.dataset.output:b.dataset.input)===task+':'+port);if(!button)return null;const rect=button.getBoundingClientRect(),world=$('world').getBoundingClientRect(),bounds=card.getBoundingClientRect();return{x:((output?bounds.right:bounds.left)-world.left)/scale,y:(rect.top+rect.height/2-world.top)/scale};}

function drawEdges(){const svg=$('edges');svg.replaceChildren();const ns='http://www.w3.org/2000/svg',defs=document.createElementNS(ns,'defs'),marker=document.createElementNS(ns,'marker');marker.setAttribute('id','arrow');marker.setAttribute('viewBox','0 0 10 10');marker.setAttribute('refX','9');marker.setAttribute('refY','5');marker.setAttribute('markerWidth','7');marker.setAttribute('markerHeight','7');marker.setAttribute('orient','auto');const tip=document.createElementNS(ns,'path');tip.setAttribute('d','M 0 0 L 10 5 L 0 10 z');tip.style.fill='#7192c3';tip.style.stroke='none';marker.append(tip);defs.append(marker);svg.append(defs);
 function edge(from,z,selection,label){const b=binding(from),a=portPoint(b.task,b.port,true);if(!a||!z)return;const curve=document.createElementNS(ns,'path'),d=Math.max(70,Math.abs(z.x-a.x)*.45);curve.setAttribute('d',`M${a.x},${a.y} C${a.x+d},${a.y} ${z.x-d},${z.y} ${z.x},${z.y}`);curve.setAttribute('marker-end','url(#arrow)');if(selection.output){curve.dataset.jobOutputEdge=selection.output;curve.classList.add('job-output-edge');}else curve.dataset.edge=selection.task+':'+selection.port;curve.classList.toggle('selected',JSON.stringify(selectedEdge)===JSON.stringify(selection));curve.setAttribute('tabindex','0');curve.setAttribute('role','button');curve.setAttribute('aria-label','Connection '+from+' to '+(selection.output?'Outlet: '+selection.output:selection.task+':'+selection.port));const select=e=>{e.stopPropagation();selectedNodeRegion=null;selectedEdge=selection;pendingPort=null;selectedSteps.clear();selected=null;outletSelected=false;render();$('canvas').focus({preventScroll:true});notice('Connection selected. Press Delete / Backspace to remove.');};curve.addEventListener('click',select);curve.addEventListener('keydown',e=>{if(e.key==='Enter'||e.key===' '){e.preventDefault();select(e);}});svg.append(curve);const text=document.createElementNS(ns,'text');text.setAttribute('x',(a.x+z.x)/2);text.setAttribute('y',(a.y+z.y)/2-8);text.setAttribute('text-anchor','middle');text.textContent=label;svg.append(text);}
 for(const t of job.tasks)for(const [port,from]of Object.entries(t.inputs||{}))edge(from,portPoint(t.id,port,false),{task:t.id,port},binding(from).port+' → '+port);
 for(const [name,from]of Object.entries(job.outputs))edge(from,outletPoint(),{output:name},name);
 drawPreview();
}
function defaultOutletPosition(){return{x:Math.max(280,Math.min(800,$('canvas').clientWidth/scale-190)),y:280};}
function renderOutlet(){if(!outletPosition)outletPosition=defaultOutletPosition();$('outlet').style.left=outletPosition.x+'px';$('outlet').style.top=outletPosition.y+'px';$('outlet').classList.toggle('selected',outletSelected);const count=Object.keys(job.outputs).length;$('outlet-count').textContent=count+' Job output'+(count===1?'':'s');}
function outletPoint(){const rect=$('outlet-input').getBoundingClientRect(),world=$('world').getBoundingClientRect();return{x:(rect.left-world.left)/scale,y:(rect.top+rect.height/2-world.top)/scale};}
function selectOutlet(append=false){selectedNodeRegion=null;if(!append)selectedSteps.clear();selected=null;selectedEdge=null;outletSelected=true;render();$('canvas').focus({preventScroll:true});}
$('outlet').addEventListener('click',guard(async e=>{e.stopPropagation();if(pendingPort)await connectOutlet();else if($('outlet').dataset.handled!=='true')selectOutlet(e.shiftKey);delete $('outlet').dataset.handled;}));
$('outlet-handle').addEventListener('pointerdown',e=>startDrag(e,null,$('outlet')));

function nodeAt(x,y){return [...laneBounds].reverse().find(r=>x>=r.x&&x<=r.x+r.width&&y>=r.y&&y<=r.y+r.height)?.id;}
function addNodeArea(id,x,y){requireConnected();if(!nodes.some(n=>n.id===id&&(!n.state||n.state==='approved')))throw Error('Select an available Union member.');if(nodeAreas[id]||job.tasks.some(t=>t.node===id)){notice('This node already has a canvas region.');return;}remember();nodeAreas[id]={x:Math.max(20,x),y:Math.max(30,y)};changed();notice('Place steps inside '+nodeLabel(id)+' to assign them. Steps outside node regions omit node.');}
function startRegionDrag(e,id,lane,handle){if(e.button!==0||pendingPort)return;graphInteraction=true;e.preventDefault();e.stopPropagation();selectedNodeRegion=id;selected=null;selectedSteps.clear();outletSelected=false;selectedEdge=null;document.querySelectorAll('.lane').forEach(n=>n.classList.toggle('selected',n.dataset.nodeRegion===id));document.querySelectorAll('.card.selected,#outlet.selected').forEach(n=>n.classList.remove('selected'));const regions=laneBounds.map(r=>({...r})),layout=dragLayout(),r={...laneBounds.find(r=>r.id===id)},before=snapshot(),start={x:e.clientX,y:e.clientY};let moved=false;handle.setPointerCapture(e.pointerId);
 function move(ev){if(!moved&&Math.hypot(ev.clientX-start.x,ev.clientY-start.y)>3)moved=true;if(!moved)return;
 const dx=Math.max((ev.clientX-start.x)/scale,20-r.x),dy=Math.max((ev.clientY-start.y)/scale,30-r.y);
 positions=clone(before.positions);nodeAreas=clone(before.nodeAreas);outletPosition=clone(before.outletPosition);
 const regionIds=new Set(regions.map(area=>area.id));
 const rects=regions.map(area=>({...area,kind:'region',fixed:area.id===id,x:area.x+(area.id===id?dx:0),y:area.y+(area.id===id?dy:0)}));
 for(const step of layout){if(!regionIds.has(job.tasks.find(t=>t.id===step.id)?.node))rects.push({...step,kind:'step',fixed:false});}
 rects.push({...outletRect(),kind:'outlet',fixed:false});avoidRectangles(rects,dy<0?-1:1);
 for(const item of rects){if(item.kind==='region'){
   const origin=regions.find(area=>area.id===item.id),shift={x:item.x-origin.x,y:item.y-origin.y};
   if(item.fixed||shift.x||shift.y)nodeAreas[item.id]={x:item.x,y:item.y};
   const element=[...$('lanes').children].find(n=>n.dataset.nodeRegion===item.id);element.style.left=item.x+'px';element.style.top=item.y+'px';
   for(const step of job.tasks.filter(t=>t.node===item.id)){const p=before.positions[step.id];positions[step.id]={x:p.x+shift.x,y:p.y+shift.y};}
 }else if(item.kind==='step')positions[item.id]={x:item.x,y:item.y};else outletPosition={x:item.x,y:item.y};}
 for(const step of layout){step.card.style.left=positions[step.id].x+'px';step.card.style.top=positions[step.id].y+'px';}
 keepOutletOutsideRegions(rects.filter(item=>item.kind==='region'));worldExtent([...rects,outletRect()]);drawEdges();}
 function end(ev){graphInteraction=false;handle.removeEventListener('pointermove',move);handle.removeEventListener('pointerup',end);handle.removeEventListener('pointercancel',end);if(ev.type==='pointercancel'&&moved){restoreDragLayout(before);}else if(moved){recordSnapshot(before);changed();notice('Node region and its steps moved.');}else{render();notice('Node region selected. Drag its title to move it with its steps.');}$('canvas').focus({preventScroll:true});}
 handle.addEventListener('pointermove',move);handle.addEventListener('pointerup',end);handle.addEventListener('pointercancel',end);
}
function nodeRegionBounds(){const groups=[...new Set([...Object.keys(nodeAreas),...job.tasks.map(t=>t.node).filter(Boolean)])];return groups.map(id=>{const ts=job.tasks.filter(t=>t.node===id),p=nodeAreas[id],xs=ts.map(t=>point(t).x),ys=ts.map(t=>point(t).y),left=Math.min(...xs.map(x=>x-20),...(p?[p.x]:[])),top=Math.min(...ys.map(y=>y-40),...(p?[p.y]:[]));return{id,x:left,y:top,width:Math.max(650,...xs.map(x=>x-left+255)),height:Math.max(250,...ys.map(y=>y-top+195))};});}
function keepOutletOutsideRegions(regions=nodeRegionBounds()){
 const rect=outletRect(),steps=dragLayout(),obstacles=[...regions,...steps];let obstacle;
 // Move only Outlet, monotonically rightward, until it clears whole containers and cards.
 while((obstacle=obstacles.find(r=>overlaps(rect,r))))rect.x=obstacle.x+obstacle.width+18;
 outletPosition={x:rect.x,y:rect.y};renderOutlet();worldExtent([...regions,...steps,rect]);
}
function drawLanes(){const host=$('lanes');host.replaceChildren();laneBounds=[];for(const [i,area]of nodeRegionBounds().entries()){const {id,x:left,y:top,width,height}=area,ts=job.tasks.filter(t=>t.node===id);laneBounds.push(area);const lane=el('div',undefined,'lane'+(selectedNodeRegion===id?' selected':''));const handle=el('button',undefined,'lane-handle');const icon=document.createElementNS('http://www.w3.org/2000/svg','svg');icon.setAttribute('viewBox','0 0 24 24');icon.setAttribute('aria-hidden','true');const path=document.createElementNS(icon.namespaceURI,'path');path.setAttribute('d','M4 3h16v7H4zM4 14h16v7H4zM7 6.5h.01M7 17.5h.01M11 6.5h6M11 17.5h6M12 10v4');icon.append(path);handle.append(icon,el('span',nodeLabel(id),'lane-address'),el('span',ts.length+' '+(ts.length===1?'step':'steps'),'lane-count'));handle.type='button';handle.title='Drag to move this node region and its steps';handle.addEventListener('pointerdown',e=>startRegionDrag(e,id,lane,handle));handle.addEventListener('click',e=>e.stopPropagation());lane.append(handle);lane.dataset.nodeRegion=id;lane.style.setProperty('--accent',nodeColor(id));Object.assign(lane.style,{left:left+'px',top:top+'px',width:width+'px',height:height+'px'});host.append(lane);}}

function inspect(){$('duplicate-step').disabled=selectedSteps.size!==1||outletSelected;$('step-save-status').textContent='';const multiple=selectedSteps.size+(outletSelected?1:0)>1;const selection=multiple?null:outletSelected?$('outlet'):selected;if(selection!==inspectedSelection){inspectedSelection=selection;if(selection&&outletSelected&&!multiple)setInspector(true);}$('inspector-empty').textContent=multiple?(selectedSteps.size+(outletSelected?1:0))+' elements selected. Drag to move together; Delete removes selected steps.':outletSelected?'Outlet collects final Job result streams. It can be moved, but cannot be deleted or duplicated.':'Select a step to edit its properties and parameters.';const t=multiple?null:job.tasks.find(t=>t.id===selected);$('properties').hidden=!t;$('inspector-empty').hidden=!!t;refreshStepOutputs();if(!t)return;$('step-id').value=t.id;$('step-title').value=t.title||'';$('step-operator').value=t.operator;refreshStepOutputs();const schema=parameterSchema(t);parameterEditor=ParameterEditor.mount($('step-params'),t.params||{},schema);if(!schema){$('step-params').prepend(el('p','Parameter definitions unavailable. Reload the operator repository.','parameter-error'));}$('inputs').replaceChildren();for(const port of inputPorts(t)){const row=el('div',undefined,'input-row');row.append(el('span',port+' ← '+(t.inputs[port]||'not connected')));if(t.inputs[port]){const remove=el('button','×');remove.addEventListener('click',()=>{remember();delete t.inputs[port];changed();});row.append(remove);}$('inputs').append(row);}}

async function connectPort(t,port){if(!pendingPort)return;const rev=revision;const candidate=clone(job);candidate.tasks.find(x=>x.id===t.id).inputs[port]=pendingPort;const from=pendingPort;pendingPort=null;drawPreview();try{await api('document',{...payload(),job_yaml:JSON.stringify(candidate)});if(rev!==revision)throw Error('Draft changed while creating the connection. Please try again.');remember();job=candidate;changed();notice('Connected '+from+' → '+t.id+':'+port);}catch(e){notice(e.message,true);render();}}
function arrange(){nodeAreas={};const ranks={},visiting=new Set();function rank(t){if(ranks[t.id]!==undefined)return ranks[t.id];if(visiting.has(t.id))return 0;visiting.add(t.id);const inputs=Object.values(t.inputs||{}).map(v=>job.tasks.find(x=>x.id===binding(v).task)).filter(Boolean);const r=inputs.length?Math.max(...inputs.map(rank))+1:0;visiting.delete(t.id);return ranks[t.id]=r;}const groups=[...new Set(job.tasks.map(t=>t.node||''))],slots={};for(const t of job.tasks){const g=groups.indexOf(t.node||''),r=rank(t),key=g+':'+r,slot=slots[key]||0;slots[key]=slot+1;positions[t.id]={x:65+r*340,y:85+g*330+slot*190};}outletPosition={x:Math.min(5800,Math.max(400,...job.tasks.map(t=>point(t).x+350))),y:120};render();layoutChanged();}
function zoom(value){scale=Math.max(.35,Math.min(1.6,value));$('world').style.transform=`scale(${scale})`;$('zoom').value=Math.round(scale*100)+'%';drawPreview();layoutChanged();}
function showTab(name){setDetails(true);if(name==='logs')renderLogs();document.querySelectorAll('.tab').forEach(e=>e.hidden=e.id!=='tab-'+name);document.querySelectorAll('[data-tab]').forEach(e=>e.classList.toggle('active',e.dataset.tab===name));if(name==='yaml')jobYamlEditor.refresh();if(name==='variables')variablesEditor.refresh();}
function readableSchema(d){let schema=d?.schema_json||d?.schema;if(typeof schema==='string'){try{return JSON.parse(schema);}catch{return null;}}return schema;}
function showSchema(data){for(const [id,result]of Object.entries(data.tasks||{}))for(const d of result.diagnostics||[])logMessage(id+': '+d.message,d.severity,'Schema');refreshStepOutputs();}
function refreshStepOutputs(){
 const host=$('step-output-preview');host.replaceChildren();const t=job.tasks.find(t=>t.id===selected);
 if(!t){$('output-preview-status').textContent='Select a step to view its expected outputs.';return;}
 const result=report?.tasks?.[t.id];$('output-preview-status').textContent=result?'Expected output ports · samples supplied by operators (up to 10 rows / 16 KiB per port).':inferenceState;
 for(const diagnostic of result?.diagnostics||[]){host.append(el('p',diagnostic.message,diagnostic.severity==='error'?'parameter-error':'muted'));}
 for(const port of outputPorts(t)){
  const d=result?.outputs?.[port],box=el('article',undefined,'output-preview-port');box.dataset.previewPort=port;box.append(el('h3',port));
  if(!d){box.append(el('p',result?.diagnostics?.some(x=>x.severity==='error')?'Inference unavailable for this step.':inferenceState,'muted'));host.append(box);continue;}
  box.append(el('p',[...(d.representations||[]),...(d.formats||[]),d.certainty].filter(Boolean).join(' · '),'muted'));
  const schema=readableSchema(d);
  if(schema?.fields){const table=el('table');for(const f of schema.fields){const row=el('tr');row.append(el('td',f.name),el('td',SchemaTypes.fieldType(f)));table.append(row);}box.append(table);const full=el('details');full.append(el('summary','Complete schema'),el('pre',JSON.stringify(schema,null,2)));box.append(full);}
  else box.append(el('p',(d.representations||[]).includes('records')?'Schema unknown.':'Byte stream: no record schema.','muted'));
  if(d.structure){const structure=el('details');structure.append(el('summary','Structured representation'),el('pre',JSON.stringify(d.structure,null,2)));box.append(structure);}
  box.append(el('h4','Output sample'));
  if(d.sample){const sample=d.sample;box.append(el('p',sample.representation+' · '+sample.format+(sample.truncated?' · truncated':''),'muted'));if(sample.representation==='records'){
   const rows=sample.rows||[],columns=[...new Set([...(schema?.fields||[]).map(f=>f.name),...rows.flatMap(row=>Object.keys(row))])];
   if(rows.length&&columns.length){const scroll=el('div',undefined,'output-sample-scroll'),table=el('table',undefined,'output-sample-table'),head=el('thead'),header=el('tr'),body=el('tbody');table.setAttribute('aria-label','Output sample records');for(const name of columns){const th=el('th',name);th.scope='col';th.title=name;header.append(th);}head.append(header);
    for(const row of rows){const tr=el('tr');for(const name of columns){const value=row[name],text=value===null?'null':value===undefined?'':typeof value==='object'?JSON.stringify(value):String(value),td=el('td',text);td.title=text;if(value===null)td.classList.add('sample-null');tr.append(td);}body.append(tr);}table.append(head,body);scroll.append(table);box.append(scroll);
   }else box.append(el('p','No sample rows.','muted'));
  }else{box.append(el('pre',sample.data_base64||'','output-sample'),el('small','Binary sample encoded as Base64.'));}}
  else box.append(el('p',inferenceRunning?'Loading output sample…':'Sample unavailable for current inputs.','muted'));
  host.append(box);
 }
 if(result&&!outputPorts(t).length)host.append(el('p','This step has no output ports.','muted'));
}

async function loadDocument(text,vars=variablesEditor.getText()){const n=await api('document',{job_yaml:text,variables_yaml:vars});remember();cancelConnection();selectedEdge=null;job=n.job;automaticReadViews=new Set(job.tasks.filter(t=>t.operator==='read_item@1.0'&&!Object.hasOwn(t.params||{},'view')).map(t=>t.id));for(const t of job.tasks)if(automaticReadViews.has(t.id))t.params.view='records';const layout=job.designer;delete job.designer;positions=layout?.positions?Object.fromEntries(job.tasks.filter(t=>Object.hasOwn(layout.positions,t.id)).map(t=>[t.id,clone(layout.positions[t.id])])):{};nodeAreas=clone(layout?.node_areas||{});selectedNodeRegion=null;outletPosition=layout?.outlet?clone(layout.outlet):null;outletSelected=false;selected=null;selectedSteps.clear();variablesEditor.setText(n.variables_yaml);yamlEdited=false;invalidate();if(layout)render();else arrange();restoreViewport(layout?.viewport);await syncYaml();persist();notice('Loaded '+job.tasks.length+' steps'+(layout?' with saved layout.':'.'));}

let exportDocuments=null,exportFilename='job.yaml',exportBlobUrl=null;
function selectedExport(){return exportDocuments[$('export-designer').checked?'withLayout':'execution'];}
function updateExportPreview(){exportYamlEditor.setText(selectedExport());if(exportBlobUrl){const previous=exportBlobUrl;setTimeout(()=>URL.revokeObjectURL(previous),2000);}exportBlobUrl=URL.createObjectURL(new Blob([selectedExport()],{type:'application/yaml'}));$('export-download').href=exportBlobUrl;$('export-download').download=exportFilename;$('export-status').textContent='';}
async function openExportPreview(){
 const request=payload();
 const full=await api('export',{...request,include_designer:true});
 const plain=await api('export',{...request,include_designer:false});
 exportDocuments={withLayout:full.job_yaml,execution:plain.job_yaml};
 exportFilename=(job.name.replace(/[^a-zA-Z0-9_-]+/g,'-')||'job')+'.yaml';
 $('export-designer').checked=true;updateExportPreview();$('export-dialog').showModal();exportYamlEditor.refresh();
}
$('export-designer').addEventListener('change',updateExportPreview);
$('export-close').addEventListener('click',()=>$('export-dialog').close());
$('export-download').addEventListener('click',()=>{$('export-status').textContent='Download requested.';});
$('export-copy').addEventListener('click',guard(async()=>{
 const text=selectedExport();
 if(navigator.clipboard&&window.isSecureContext)await navigator.clipboard.writeText(text);
 else {const field=el('textarea');field.value=text;field.style.position='fixed';field.style.opacity='0';$('export-dialog').append(field);field.select();const copied=document.execCommand('copy');field.remove();if(!copied)throw Error('Copy unavailable. Select the YAML preview to copy manually.');}
 $('export-status').textContent='Copied.';
}));
$('export-print').addEventListener('click',()=>{
 const page=window.open('','_blank');if(!page){$('export-status').textContent='Allow popups to print the YAML.';return;}
 page.document.title=exportFilename;
 const pre=page.document.createElement('pre');pre.textContent=selectedExport();pre.style.cssText='white-space:pre-wrap;overflow-wrap:anywhere;font:11px/1.5 monospace';page.document.body.append(pre);page.document.close();page.focus();page.print();
});
function download(name,text){const a=el('a');a.href=URL.createObjectURL(new Blob([text],{type:'application/yaml'}));a.download=name;($('export-dialog').open?$('export-dialog'):document.body).append(a);a.click();a.remove();setTimeout(()=>URL.revokeObjectURL(a.href),2000);}
function nodeCard(n){
 const e=el('button',undefined,'node-badge node-card'),online=!n.state||n.state==='approved';e.disabled=!online;e.draggable=online;
 const head=el('div',undefined,'node-card-head');head.append(operatorIcon({path:'M4 3h16v7H4zM4 14h16v7H4zM7 6h.1M7 17h.1M11 6h6M11 17h6',color:nodeColor(n.id)}),el('strong',nodeLabel(n.id)),el('span',online?'Online':'Unavailable','node-health'+(online?'':' offline')));e.append(head);
 const metrics=el('dl',undefined,'node-metrics');const add=(name,value)=>{metrics.append(el('dt',name),el('dd',String(value)));};
 add('Task slots',n.reserved_tasks===undefined?'— / '+n.capacity:n.reserved_tasks+' / '+n.capacity);
 if(n.union_capacity!==undefined)add('Union slots',(n.union_reserved_tasks??'—')+' / '+n.union_capacity);
 const r=n.resources||{};if(r.processors!==undefined)add('CPU cores',r.processors);if(r.cpu_load!==undefined)add('CPU usage',(r.cpu_load*100).toFixed(1)+'%');
 const bytes=v=>v>=1073741824?(v/1073741824).toFixed(1)+' GB':(v/1048576).toFixed(0)+' MB';
 if(r.memory_used_bytes!==undefined)add('Memory',bytes(r.memory_used_bytes)+' / '+bytes(r.memory_total_bytes));
 if(r.heap_used_bytes!==undefined)add('JVM heap',bytes(r.heap_used_bytes)+(r.heap_max_bytes?' / '+bytes(r.heap_max_bytes):''));
 if(n.uptime_ms!==undefined){const seconds=Math.floor(n.uptime_ms/1000);add('Uptime',Math.floor(seconds/3600)+'h '+Math.floor(seconds%3600/60)+'m '+seconds%60+'s');}
 if(n.started_at_ms)add('Started',new Date(n.started_at_ms).toLocaleString());
 if(n.executors?.length)add('Runtimes',n.executors.join(', '));e.append(metrics);
 if(n.web_url)e.append(el('small','Dashboard: '+n.web_url));
 if(n.observed_at_ms)e.append(el('small','Checked '+new Date(n.observed_at_ms).toLocaleTimeString()));
 return e;
}
async function connect(){connected=null;nodes=[];$('connection').textContent='Connecting…';try{const data=await api('nodes',{});nodes=data.nodes||[];connected=true;$('connection').textContent='Connected · '+nodes.length+' nodes';$('nodes').replaceChildren(...nodes.map((n,i)=>{const e=nodeCard(n);e.type='button';e.dataset.node=n.id;e.title='Drag onto the canvas to create a node region';e.addEventListener('dragstart',ev=>ev.dataTransfer.setData('application/dacp-node',n.id));e.addEventListener('click',guard(()=>addNodeArea(n.id,40,Math.max(40,...laneBounds.map(r=>r.y+r.height+40)))));e.style.setProperty('--accent',colors[i%colors.length]);return e;}));if(!nodes.length)$('nodes').textContent='Single-node server · no step node assignment needed';render();notice('Connected. Execution placements come from this server.');if($('library-datasets-tab').getAttribute('aria-selected')==='true')ensureDatasets();scheduleInference();}catch(e){connected=false;if($('library-datasets-tab').getAttribute('aria-selected')==='true')ensureDatasets();$('connection').textContent='Server unavailable · reload to retry';throw e;}}
function requireConnected(){if(!connected)throw Error('Configured server is unavailable. Reload after recovery.');}
let datasetRequest=null,datasetsLoaded=false;
function ensureDatasets(){if(datasetsLoaded||datasetRequest)return;if(!connected){$('datasets').textContent=connected===false?'Configured server unavailable. Reload after recovery.':'Waiting for the configured server…';return;}guard(datasets)();}
async function datasets(){requireConnected();if(datasetRequest)return datasetRequest;$('datasets-refresh').disabled=true;
 datasetRequest=(async()=>{try{$('datasets').textContent='Loading datasets…';const data=await api('datasets',{});$('datasets').replaceChildren();if(!data.complete)logMessage('Partial dataset directory: '+data.unavailable_sources+' sources unavailable.','warning','Datasets');if(!data.complete)$('datasets').append(el('p','Partial directory: '+data.unavailable_sources+' sources unavailable.','muted'));for(const d of data.datasets){const box=el('div',undefined,'dataset'),button=el('button',undefined,'dataset-button'),children=el('div');button.setAttribute('aria-label',d.name);button.setAttribute('aria-expanded','false');button.append(el('span','▸','dataset-chevron'),operatorIcon({path:'M4 6c0-4 16-4 16 0s-16 4-16 0v12c0 4 16 4 16 0V6M4 12c0 4 16 4 16 0',color:'#5683a6'}),el('span',d.name));children.className='dataset-children';children.hidden=true;button.addEventListener('click',guard(async()=>{if(button.getAttribute('aria-expanded')==='true'){button.setAttribute('aria-expanded','false');children.hidden=true;return;}button.setAttribute('aria-expanded','true');children.hidden=false;if(children.childElementCount)return;button.disabled=true;try{await browseItems(d.uri,'/',children);}catch(e){children.textContent='Items unavailable. Collapse and expand to retry.';throw e;}finally{button.disabled=false;}}));box.append(button,children);$('datasets').append(box);}if(!data.datasets.length)$('datasets').append(el('p','No datasets available.','muted'));datasetsLoaded=true;}catch(e){$('datasets').textContent='Dataset directory unavailable. Use Refresh datasets to retry.';throw e;}finally{$('datasets-refresh').disabled=false;datasetRequest=null;}})();return datasetRequest;}
async function browseItems(uri,path,host){host.textContent='Loading…';const result=await api('items',{uri,path});host.replaceChildren();for(const item of result.items){const row=el('div',(item.kind==='folder'?'▸ ':'↳ ')+item.path+' · '+item.views.join('/'),'item');row.tabIndex=0;const nested=el('div');row.addEventListener('click',guard(async()=>{if(item.kind==='folder'){if(nested.childElementCount)nested.replaceChildren();else await browseItems(uri,item.path,nested);return;}let task=job.tasks.find(t=>t.id===selected&&t.operator==='read_item@1.0');if(!task){addOperator({name:'read_item',version:'1.0'},60,85+job.tasks.length*35);task=job.tasks.find(t=>t.id===selected);}remember();task.params={uri:uri.replace(/\/$/,'')+item.path,view:item.views.includes('records')?'records':'bytes'};changed();notice('Data source selected: '+task.params.uri);}));host.append(row,nested);}}
$('canvas').addEventListener('dragover',e=>e.preventDefault());$('canvas').addEventListener('drop',e=>{e.preventDefault();const rect=$('world').getBoundingClientRect(),x=(e.clientX-rect.left)/scale,y=(e.clientY-rect.top)/scale,id=e.dataTransfer.getData('application/dacp-node');if(id){guard(()=>addNodeArea(id,x,y))();return;}const ref=e.dataTransfer.getData('application/dacp-operator');const op=ref==='read_item@1.0'?{name:'read_item',version:'1.0'}:operators.find(o=>o.name+'@'+o.version===ref);if(!op)return;addOperator(op,x,y);});
$('search').addEventListener('input',renderLibrary);$('job-name').addEventListener('change',()=>{remember();job.name=$('job-name').value;changed();});
async function loadOperators(){$('repository-state').textContent='Loading operators…';try{operators=await api('operators',{});renderLibrary();render();$('repository-state').textContent=operators.length+' published versions';}catch(e){$('repository-state').textContent='Repository unavailable · reload to retry';throw e;}}
$('datasets-refresh').addEventListener('click',guard(datasets));

function libraryTab(name){for(const key of ['operators','nodes','datasets']){const active=key===name;const button=$('library-'+key+'-tab');$('library-'+key+'-panel').hidden=!active;button.setAttribute('aria-selected',String(active));button.tabIndex=active?0:-1;}hideOperatorTip();if(name==='datasets')ensureDatasets();}
for(const button of document.querySelectorAll('[data-library-tab]')){button.addEventListener('click',()=>libraryTab(button.dataset.libraryTab));button.addEventListener('keydown',e=>{if(!['ArrowUp','ArrowDown','Home','End'].includes(e.key))return;e.preventDefault();const keys=['operators','nodes','datasets'],index=keys.indexOf(button.dataset.libraryTab),name=keys[e.key==='Home'?0:e.key==='End'?2:(index+(e.key==='ArrowDown'?1:2))%3];libraryTab(name);$('library-'+name+'-tab').focus();});}
function inspectorTab(name){for(const key of ['properties','parameters','outputs']){const active=key===name;$(key+'-panel').hidden=!active;$(key+'-tab').setAttribute('aria-selected',String(active));$(key+'-tab').tabIndex=active?0:-1;}if(name==='outputs')refreshStepOutputs();}
for(const button of document.querySelectorAll('[data-inspector-tab]')){button.addEventListener('click',()=>inspectorTab(button.dataset.inspectorTab));button.addEventListener('keydown',e=>{if(['ArrowLeft','ArrowRight','Home','End'].includes(e.key)){e.preventDefault();const keys=['properties','parameters','outputs'],i=keys.indexOf(button.dataset.inspectorTab),name=keys[e.key==='Home'?0:e.key==='End'?2:(i+(e.key==='ArrowRight'?1:2))%3];inspectorTab(name);$(name+'-tab').focus();}});}
function setInspector(open){$('inspector').hidden=!open;document.querySelector('main').classList.toggle('inspector-open',open);const toggle=$('toggle-inspector-panel');toggle.textContent=open?'>':'<';toggle.setAttribute('aria-expanded',String(open));toggle.setAttribute('aria-label',open?'Collapse properties sidebar':'Expand properties sidebar');}
function setDetails(open){document.querySelector('.bottom').classList.toggle('collapsed',!open);$('toggle-details').setAttribute('aria-expanded',String(open));$('toggle-details').setAttribute('aria-label',open?'Collapse editor panel':'Expand editor panel');$('toggle-details').textContent=open?'⌄':'⌃';if(open){jobYamlEditor.refresh();variablesEditor.refresh();}}
$('toggle-inspector-panel').addEventListener('click',()=>setInspector($('inspector').hidden));
$('toggle-library').addEventListener('click',()=>{const open=$('library').hidden;$('library').hidden=!open;document.querySelector('main').classList.toggle('library-collapsed',!open);const toggle=$('toggle-library');toggle.textContent=open?'<':'>';toggle.setAttribute('aria-expanded',String(open));toggle.setAttribute('aria-label',open?'Collapse resources sidebar':'Expand resources sidebar');hideOperatorTip();});
$('toggle-details').addEventListener('click',()=>setDetails(document.querySelector('.bottom').classList.contains('collapsed')));
$('canvas').addEventListener('click',e=>{if(suppressCanvasClick)return;if(!e.target.closest('.card,#outlet,.lane-handle')&&!e.target.closest('#edges path')){selected=null;selectedSteps.clear();selectedNodeRegion=null;selectedEdge=null;outletSelected=false;render();}});
document.addEventListener('keydown',e=>{if(e.key==='Escape'){cancelConnection();}});
let lastStepEdit=null;
function saveStepEdits(){
 const t=job.tasks.find(t=>t.id===selected);if(!t)return;
 try{
  if(!parameterSchema(t))throw Error('Parameter definitions unavailable.');
  const params=parameterEditor.read(),title=$('step-title').value;
  if(JSON.stringify(params)===JSON.stringify(t.params||{})&&title===(t.title||'')){$('step-save-status').textContent='';return;}
  const now=Date.now();if(!lastStepEdit||lastStepEdit.id!==t.id||now-lastStepEdit.at>750||lastStepEdit.revision!==revision)remember();
  t.title=title;t.params=params;invalidate();lastStepEdit={id:t.id,at:now,revision};persist();
  const card=[...$('cards').children].find(c=>c.dataset.task===t.id);if(card)card.querySelector('.step-heading strong').textContent=t.title||t.id;
  $('undo').disabled=!undo.length;$('redo').disabled=!redo.length;
  $('step-save-status').textContent='';clearTimeout(yamlTimer);yamlTimer=setTimeout(()=>guard(syncYaml)(),250);
 }catch(e){$('step-save-status').textContent='Not saved: '+e.message;}
}
$('step-title').addEventListener('input',saveStepEdits);

function duplicateStep(){
 if(selectedSteps.size!==1||outletSelected)return;const original=job.tasks.find(t=>t.id===[...selectedSteps][0]);if(!original)return;
 const before=snapshot();let i=1;while(job.tasks.some(t=>t.id==='step_'+i))i++;const copy={...clone(original),id:'step_'+i,title:(original.title||original.id)+' copy',inputs:{}};
 const p=point(original);job.tasks.push(copy);positions[copy.id]={x:p.x+50,y:p.y+50};if(automaticReadViews.has(original.id))automaticReadViews.add(copy.id);
 selected=copy.id;selectedSteps=new Set([copy.id]);selectedEdge=null;outletSelected=false;render();avoidSteps(dragLayout(),new Set([copy.id]),false);recordSnapshot(before);changed();notice('Step duplicated. Reconnect its input ports.');
}
$('duplicate-step').addEventListener('click',duplicateStep);
document.addEventListener('keydown',e=>{if((e.ctrlKey||e.metaKey)&&!e.altKey&&!e.repeat&&e.key.toLowerCase()==='d'&&!e.target.closest('input,textarea,select,[contenteditable]')){e.preventDefault();duplicateStep();}});
function deleteSelectedStep(){const ids=new Set(selectedSteps);if(selected)ids.add(selected);if(!ids.size)return;if(ids.size>1&&!window.confirm('Delete '+ids.size+' selected steps and their connections? This can be undone.')){notice('Deletion cancelled.');return;}remember();job.tasks=job.tasks.filter(t=>!ids.has(t.id));for(const t of job.tasks)for(const [p,v]of Object.entries(t.inputs||{}))if(ids.has(binding(v).task))delete t.inputs[p];for(const [p,v]of Object.entries(job.outputs))if(ids.has(binding(v).task))delete job.outputs[p];for(const id of ids)delete positions[id];selected=null;selectedSteps.clear();selectedEdge=null;cancelConnection();changed();$('canvas').focus({preventScroll:true});notice('Selected steps deleted. Undo to restore steps and connections.');}


function outputNames(task,port){return Object.entries(job.outputs).filter(([,v])=>{const b=binding(v);return b.task===task&&b.port===port;}).map(([name])=>name);}
async function updateJobOutputs(outputs){const rev=revision;const candidate={...clone(job),outputs};await api('document',{...payload(),job_yaml:JSON.stringify(candidate)});if(rev!==revision)throw Error('Draft changed while updating Job outputs. Please try again.');remember();job=candidate;pendingPort=null;changed();}
async function connectOutlet(){const from=pendingPort;if(!from)return;const b=binding(from);if(outputNames(b.task,b.port).length){cancelConnection();selectOutlet();notice('This port is already connected to Outlet.');return;}await updateJobOutputs({result:b.task+(b.port==='result'?'':':'+b.port)});selectOutlet();notice('Outlet now receives '+from+'.');}

function deleteConnection(){if(!selectedEdge)return;if(selectedEdge.output){if(!Object.hasOwn(job.outputs,selectedEdge.output))return;remember();delete job.outputs[selectedEdge.output];}else{const t=job.tasks.find(t=>t.id===selectedEdge.task);if(!t?.inputs?.[selectedEdge.port])return;remember();delete t.inputs[selectedEdge.port];}selectedEdge=null;changed();notice('Connection deleted.');}

document.addEventListener('keydown',e=>{if(graphInteraction||e.repeat||e.target.closest('input,textarea,select,[contenteditable]')||e.ctrlKey||e.metaKey||e.altKey)return;if(outletSelected&&!selectedSteps.size&&(e.key==='Delete'||e.key==='Backspace')){e.preventDefault();notice('Outlet cannot be deleted. Remove its connections to change Job outputs.');return;}if(e.key==='Delete'||e.key==='Backspace'){if(selectedEdge){e.preventDefault();deleteConnection();}else if(selected||selectedSteps.size){e.preventDefault();deleteSelectedStep();}}});

$('arrange').addEventListener('click',()=>{remember();arrange();});$('zoom-in').addEventListener('click',()=>zoom(scale*1.15));$('zoom-out').addEventListener('click',()=>zoom(scale/1.15));$('fit').addEventListener('click',()=>{const width=Math.max(outletPosition.x+180,500,...job.tasks.map(t=>point(t).x+280)),height=Math.max(outletPosition.y+150,250,...job.tasks.map(t=>point(t).y+200));zoom(Math.min($('canvas').clientWidth/width,$('canvas').clientHeight/height));$('canvas').scrollTo(0,0);});
function undoChange(){if(graphInteraction||!undo.length)return;redo.push(snapshot());restore(undo.pop());notice('Change undone.');}
function redoChange(){if(graphInteraction||!redo.length)return;undo.push(snapshot());restore(redo.pop());notice('Change redone.');}
$('undo').addEventListener('click',undoChange);$('redo').addEventListener('click',redoChange);
document.addEventListener('keydown',e=>{if(e.target.closest('input,textarea,select,[contenteditable]')||e.altKey||!(e.ctrlKey||e.metaKey))return;const key=e.key.toLowerCase();if(key==='z'||(key==='y'&&e.ctrlKey&&!e.shiftKey)){e.preventDefault();if(key==='y'||e.shiftKey)redoChange();else undoChange();}});
$('new').addEventListener('click',()=>{remember();cancelConnection();selectedEdge=null;job={name:'Untitled job',tasks:[],outputs:{}};automaticReadViews.clear();positions={};nodeAreas={};selectedNodeRegion=null;outletPosition=null;outletSelected=false;selected=null;selectedSteps.clear();variablesEditor.setText('{}');yamlEdited=false;changed();});
$('load').addEventListener('click',()=>$('file').click());$('file').addEventListener('change',guard(async()=>{const f=$('file').files[0];if(f){if(f.size>1024*1024)throw Error('YAML file exceeds 1 MiB');await loadDocument(await f.text());}$('file').value='';}));$('apply-yaml').addEventListener('click',guard(()=>loadDocument(jobYamlEditor.getText())));$('export').addEventListener('click',guard(async()=>{if(yamlEdited)throw Error('Apply the edited YAML before exporting.');if(job.tasks.some(t=>t.node)){requireConnected();if(job.tasks.some(t=>t.node&&!nodes.some(n=>n.id===t.node)))throw Error('Update unavailable node placements using the configured Union.');}await openExportPreview();}));
$('load-vars').addEventListener('click',()=>$('vars-file').click());$('vars-file').addEventListener('change',guard(async()=>{const f=$('vars-file').files[0];if(f){if(f.size>1024*1024)throw Error('Variable file exceeds 1 MiB');const text=await f.text();await api('mapping',{yaml:text});remember();variablesEditor.setText(text);changed();}$('vars-file').value='';}));$('export-vars').addEventListener('click',guard(async()=>{const n=await api('document',payload());download('variables.yaml',n.variables_yaml);}));

for(const b of document.querySelectorAll('[data-tab]'))b.addEventListener('click',()=>showTab(b.dataset.tab));
$('chat-form').addEventListener('submit',guard(async e=>{e.preventDefault();requireConnected();const message=$('chat-input').value.trim();if(!message)return;chatHistory.push({role:'user',content:message});$('messages').append(el('div',message,'chat-message user'));$('chat-input').value='';$('chat-send').disabled=true;const version=revision;const draft=proposal?{...payload(),job_yaml:proposal.job_yaml,variables_yaml:proposal.variables_yaml}:payload();try{const result=await api('chat',{...draft,message,messages:chatHistory.slice(-12)});if(version!==revision){notice('Chat proposal discarded because the draft changed.');return;}proposal=result;$('messages').append(el('div',proposal.reply||'Proposed workflow','chat-message'));const detail=el('details'),summary=el('summary','Proposed YAML');detail.append(summary,el('pre',proposal.job_yaml));$('messages').append(detail);chatHistory.push({role:'assistant',content:proposal.reply});$('apply-chat').disabled=false;$('discard-chat').disabled=false;}finally{$('chat-send').disabled=false;}}));
$('apply-chat').addEventListener('click',guard(async()=>{if(proposal){await loadDocument(proposal.job_yaml,proposal.variables_yaml);proposal=null;$('apply-chat').disabled=true;$('discard-chat').disabled=true;notice('Chat proposal applied. Review connections and preview schemas.');}}));$('discard-chat').addEventListener('click',()=>{proposal=null;$('apply-chat').disabled=true;$('discard-chat').disabled=true;notice('Proposal discarded.');});
$('canvas').addEventListener('keydown',e=>{if(e.key==='Escape'){cancelConnection();selectedEdge=null;render();}});
guard(async()=>{config=await api('config');$('chat-mode').textContent=config.chat_provider==='builtin'?'Built-in command assistant (not an LLM). Try “create CSV filter job”, “add OPERATOR@VERSION”, “connect STEP to STEP:INPUT”, “place STEP on NODE” or “set variable NAME = VALUE”.':'Chat planner: '+config.chat_provider+' · proposals require your review before applying.';try{const saved=JSON.parse(localStorage.getItem('dacp-job-designer'));if(saved){await api('document',{job_yaml:JSON.stringify(saved.job),variables_yaml:saved.variables||'{}'});job=saved.job;automaticReadViews=new Set(saved.automaticReadViews||[]);positions=saved.positions||{};nodeAreas=saved.nodeAreas||{};outletPosition=saved.outletPosition||null;restoreViewport(saved.viewport);variablesEditor.setText(saved.variables||'{}');}}catch(e){notice('Saved draft could not be restored: '+e.message,true);}renderLibrary();render();await Promise.allSettled([guard(syncYaml)(),guard(connect)(),guard(loadOperators)()]);})();

window.addEventListener('resize',hideOperatorTip);document.querySelector('.library').addEventListener('scroll',hideOperatorTip);

// Rubber-band selection uses world coordinates, including zoom and canvas scrolling.
$('canvas').addEventListener('pointerdown',e=>{
 if(e.button!==0||pendingPort||e.target.closest('.card,#outlet,#edges path,.lane-handle'))return;
 e.preventDefault();graphInteraction=true;selectedNodeRegion=null;const canvas=$('canvas'),worldPoint=ev=>{const r=$('world').getBoundingClientRect();return{x:(ev.clientX-r.left)/scale,y:(ev.clientY-r.top)/scale};},start=worldPoint(e),base=e.shiftKey?new Set([...selectedSteps,...(selected?[selected]:[])]):new Set(),keepOutlet=e.shiftKey&&outletSelected,box=el('div',undefined,'selection-marquee');let moved=false;
 canvas.setPointerCapture(e.pointerId);
 function move(ev){const p=worldPoint(ev);if(!moved&&Math.hypot(p.x-start.x,p.y-start.y)*scale<4)return;if(!moved){moved=true;cancelConnection();$('world').append(box);}const r={x:Math.min(p.x,start.x),y:Math.min(p.y,start.y),width:Math.abs(p.x-start.x),height:Math.abs(p.y-start.y)};Object.assign(box.style,{left:r.x+'px',top:r.y+'px',width:r.width+'px',height:r.height+'px'});const inside=(x,y,w,h)=>x>=r.x&&y>=r.y&&x+w<=r.x+r.width&&y+h<=r.y+r.height;selectedSteps=new Set(base);for(const card of $('cards').children){const t=job.tasks.find(t=>t.id===card.dataset.task),p=point(t);if(inside(p.x,p.y,card.offsetWidth,card.offsetHeight))selectedSteps.add(t.id);card.classList.toggle('selected',selectedSteps.has(t.id));}outletSelected=keepOutlet||inside(outletPosition.x,outletPosition.y,$('outlet').offsetWidth,$('outlet').offsetHeight);$('outlet').classList.toggle('selected',outletSelected);selected=null;selectedEdge=null;}
 function end(ev){graphInteraction=false;canvas.removeEventListener('pointermove',move);canvas.removeEventListener('pointerup',end);canvas.removeEventListener('pointercancel',end);box.remove();if(!moved)render();if(moved){suppressCanvasClick=true;setTimeout(()=>suppressCanvasClick=false,0);selected=selectedSteps.size===1&&!outletSelected?[...selectedSteps][0]:null;inspectedSelection=outletSelected&&!selectedSteps.size?$('outlet'):selected;render();notice((selectedSteps.size+(outletSelected?1:0))+' elements selected. Drag to move; Delete removes steps.');}canvas.focus({preventScroll:true});}
 canvas.addEventListener('pointermove',move);canvas.addEventListener('pointerup',end);canvas.addEventListener('pointercancel',end);
});


function readViewEdited(e){if(e.target.closest('.parameter-row')?.dataset.parameterPath!=='params.view'||!selected)return;try{const params=parameterEditor.read();if(Object.hasOwn(params,'view'))automaticReadViews.delete(selected);else automaticReadViews.add(selected);}catch{}}
$('step-params').addEventListener('input',e=>{readViewEdited(e);saveStepEdits();});
$('step-params').addEventListener('change',e=>{readViewEdited(e);saveStepEdits();});
$('step-params').addEventListener('click',e=>{if(e.target.closest('button'))saveStepEdits();});

$('log-filter').addEventListener('change',()=>renderLogs(true));
$('log-follow').addEventListener('change',()=>{if($('log-follow').checked)$('log-output').scrollTop=0;});
$('log-clear').addEventListener('click',()=>{logEntries.length=0;renderLogs();});

$('canvas').addEventListener('scroll',layoutChanged,{passive:true});

$('validate').addEventListener('click',guard(async()=>{
 saveStepEdits();showTab('validation');const host=$('validation-results');host.replaceChildren();$('validation-summary').textContent='Checking…';
 const vars=await api('mapping',{yaml:variablesEditor.getText()});
 const issues=WorkflowValidation.check(executionDocument(),operators,report,vars,ParameterEditor.validate);
 if(yamlEdited)issues.unshift({severity:'warning',message:'Unapplied YAML edits are excluded. Apply YAML to validate them.'});
 const pending=$('step-save-status').textContent;if(pending)issues.unshift({severity:'error',step:selected,message:pending});
 const errors=issues.filter(x=>x.severity==='error').length,warnings=issues.length-errors;
 $('validation-summary').textContent=errors+' errors · '+warnings+' warnings'+(!issues.length?' · Workflow valid':'');
 for(const issue of issues){const row=el('div',undefined,'validation-issue '+issue.severity);row.append(el('span',issue.severity==='error'?'!':'?','schema-status-icon'));if(issue.step){const step=job.tasks.find(t=>t.id===issue.step),button=el('button',step?.title||issue.step);button.addEventListener('click',()=>{if(step){selectStep(step);setInspector(true);inspectorTab('outputs');[...$('cards').children].find(c=>c.dataset.task===step.id)?.scrollIntoView({block:'nearest',inline:'nearest'});}});row.append(button);}row.append(el('span',issue.message));host.append(row);}
 logMessage('Validation: '+errors+' errors, '+warnings+' warnings',errors?'warning':'info','Validation');
}));

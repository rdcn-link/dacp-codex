'use strict';
// Static parameter declarations drive the form. This editor never invokes operator code.
window.ParameterEditor = (() => {
 const variable=v=>typeof v==='string'&&/^\$[A-Za-z_][A-Za-z0-9_]*$/.test(v);
 const own=(o,k,v)=>Object.defineProperty(o,k,{value:v,enumerable:true,writable:true,configurable:true});
 const clone=v=>JSON.parse(JSON.stringify(v));
 function element(tag,text){const e=document.createElement(tag);if(text!==undefined)e.textContent=text;return e;}
 function typeOf(v){return v===null?'null':Array.isArray(v)?'array':typeof v==='object'?'object':variable(v)?'variable':typeof v;}
 function check(s,v,path){
  if(!s||variable(v))return;
  if(Array.isArray(s.type)){const candidates=s.type.filter(t=>{try{check({...s,type:t},v,path);return true;}catch{return false;}});if(!candidates.length)throw Error(path+': expected '+s.type.join(' / '));return;}
  const t=s.type,valid=t==='integer'?Number.isSafeInteger(v):t==='number'?typeof v==='number'&&Number.isFinite(v):t==='array'?Array.isArray(v):t==='object'?v!==null&&typeof v==='object'&&!Array.isArray(v):t==='null'?v===null:typeof v===t;
  if(!valid)throw Error(path+': expected '+t);
  if(s.enum&&!s.enum.some(x=>JSON.stringify(x)===JSON.stringify(v)))throw Error(path+': choose a declared value');
  const measure=t==='string'?[...v].length:t==='array'?v.length:v;
  const allowedBounds=t==='string'?['minLength','maxLength']:t==='array'?['minItems','maxItems']:['integer','number'].includes(t)?['minimum','maximum']:[];
  for(const [key,min]of [['minimum',true],['maximum',false],['minLength',true],['maxLength',false],['minItems',true],['maxItems',false]])if(allowedBounds.includes(key)&&Object.hasOwn(s,key)&&(min?measure<s[key]:measure>s[key]))throw Error(path+': violates '+key);
  if(t==='object'){for(const name of s.required||[])if(!Object.hasOwn(v,name))throw Error(path+': missing '+name);for(const [k,x]of Object.entries(v)){if(!Object.hasOwn(s.properties,k)&&!s.additionalProperties)throw Error(path+': undeclared parameter '+k);check(Object.hasOwn(s.properties,k)?s.properties[k]:typeof s.additionalProperties==='object'?s.additionalProperties:null,x,path+'.'+k);}}
  if(t==='array')v.forEach((x,i)=>check(s.items,x,path+'['+i+']'));
 }
 function valueEditor(value,schema,path,showHelp){
  const host=element('div');host.className='parameter-value';
  const mode=element('select');mode.setAttribute('aria-label','Parameter type');
  const types=schema?[...(Array.isArray(schema.type)?schema.type:[schema.type]),'variable']:['string','integer','number','boolean','null','variable','object','array'];
  for(const t of types)mode.add(new Option(t,t));
  mode.value=variable(value)?'variable':schema?(Array.isArray(schema.type)?(schema.type.includes(typeOf(value))?typeOf(value):schema.type[0]):schema.type):value===undefined?'string':typeOf(value);const body=element('div'),controls=element('div');controls.className='parameter-controls';let read;
  const single=!!schema&&!Array.isArray(schema.type);let literal=variable(value)?schema?.default:value;
  const badge=element('span');badge.className='parameter-type';
  const useVariable=element('button','$');useVariable.type='button';useVariable.className='parameter-variable';
  function updateControls(){badge.textContent=mode.value;const active=mode.value==='variable';useVariable.classList.toggle('active',active);useVariable.setAttribute('aria-pressed',String(active));useVariable.setAttribute('aria-label',active?'Use literal value':'Use variable');useVariable.title=active?'Use literal value':'Use $VAR_NAME';}
  if(single){controls.append(badge,useVariable);useVariable.addEventListener('click',()=>{if(mode.value==='variable'){mode.value=schema.type;draw(literal);}else{try{literal=read();}catch{}mode.value='variable';draw('$VAR_NAME');}updateControls();});}else controls.append(mode);
  host.append(body);
  function draw(v){body.replaceChildren();const t=mode.value;
   if(t==='object'||t==='array'){const child=collection(v,t==='array',schema?{...schema,type:t}:null,path,showHelp);body.append(child.element);read=child.read;return;}
   if(t==='null'){read=()=>null;return;}
   if(schema?.enum&&t!=='variable'){const input=element('select');input.setAttribute('aria-label','Parameter value');const empty=new Option('Select a value','');input.add(empty);schema.enum.forEach((x,i)=>input.add(new Option(JSON.stringify(x),String(i))));const i=schema.enum.findIndex(x=>JSON.stringify(x)===JSON.stringify(v));input.value=i<0?'':String(i);body.append(input);read=()=>{if(input.value==='')throw Error(path+': choose a value');return clone(schema.enum[Number(input.value)]);};return;}
   if(t==='boolean'){const input=element('select');input.setAttribute('aria-label','Parameter value');input.add(new Option('true','true'));input.add(new Option('false','false'));input.value=String(v??false);body.append(input);read=()=>input.value==='true';return;}
   const input=element(t==='string'?'textarea':'input');input.setAttribute('aria-label','Parameter value');if(t==='string'){input.rows=Math.min(5,Math.max(1,String(v??'').split('\n').length));input.addEventListener('input',()=>{input.style.height='auto';input.style.height=Math.min(150,input.scrollHeight)+'px';});}input.value=v===undefined?'':typeof v==='object'?JSON.stringify(v):String(v);if(t==='number'||t==='integer'){input.type='number';input.step=t==='integer'?'1':'any';if(schema?.minimum!==undefined)input.min=schema.minimum;if(schema?.maximum!==undefined)input.max=schema.maximum;}if(t==='variable')input.placeholder='$VAR_NAME';body.append(input);
   read=()=>{if(t==='number'||t==='integer'){const n=Number(input.value);if(!input.value.trim()||!Number.isFinite(n)||(t==='integer'&&!Number.isSafeInteger(n)))throw Error(path+': enter a valid '+t);return n;}if(t==='variable'&&!variable(input.value))throw Error(path+': variables must use $VAR_NAME');return input.value;};
  }
  mode.addEventListener('change',()=>draw(({string:'',integer:0,number:0,boolean:false,null:null,variable:'$VAR_NAME',object:{},array:[]})[mode.value]));draw(value);updateControls();
  return{element:host,controls,setValue(v){mode.value=variable(v)?'variable':schema?(Array.isArray(schema.type)?(schema.type.includes(typeOf(v))?typeOf(v):schema.type[0]):schema.type):typeOf(v);literal=v;draw(v);updateControls();},read:()=>{const v=read();check(schema,v,path);return v;}};
 }
 function collection(value,array=false,schema,path='params',showHelp=()=>{}){
  const host=element('div');host.className='parameter-collection';const rows=element('div'),add=element('button',array?'Add item':'Add parameter');add.type='button';add.className='parameter-add';host.append(rows);if(array||!schema||schema.additionalProperties)host.append(add);const entries=[];
  const props=schema?.properties||{},required=new Set(schema?.required||[]);
  function append(key,v,declared=false,present=true){
   const row=element('div');row.className='parameter-row';row.dataset.parameterPath=path+(array?'['+key+']':'.'+key);row.setAttribute('role','group');row.setAttribute('aria-label',key||'New parameter');const heading=element('div');heading.className='parameter-heading';const name=element('span',array?'['+key+']':key);name.className='parameter-name';heading.append(name);
   const def=array?schema?.items:declared?props[key]:typeof schema?.additionalProperties==='object'?schema.additionalProperties:null;let enabled=null;
   if(declared&&required.has(key)){const mark=element('span','*');mark.className='parameter-required';mark.title='Required';heading.append(mark);}
   if(declared&&!required.has(key)){enabled=element('input');enabled.type='checkbox';enabled.checked=present;enabled.setAttribute('aria-label','Set '+key);enabled.title='Include this optional parameter';heading.prepend(enabled);}
   let remove;if(!declared){remove=element('button','×');remove.type='button';remove.setAttribute('aria-label',array?'Remove item':'Remove parameter');remove.className='parameter-remove';}
   row.append(heading);
   if(def){const info=[def.title,def.description,required.has(key)?'Required':'Optional',Array.isArray(def.type)?def.type.join(' / '):def.type];if(def.default!==undefined)info.push('Default: '+JSON.stringify(def.default));for(const k of ['minimum','maximum','minLength','maxLength','minItems','maxItems'])if(Object.hasOwn(def,k))info.push(k+': '+def[k]);if(def.enum)info.push('Options: '+def.enum.map(x=>JSON.stringify(x)).join(', '));const help=element('button','?');help.type='button';help.className='parameter-help';help.setAttribute('aria-label','Show help for '+path+'.'+key);help.setAttribute('aria-describedby','parameter-description');const display=()=>showHelp(path+(array?'['+key+']':'.'+key),info.filter(Boolean).join('\n'));for(const event of ['mouseenter','focus','click'])help.addEventListener(event,display);heading.append(help);}
   else if(schema&&!array&&!schema.additionalProperties){const warning=element('p','Undeclared parameter. Remove it before applying.');warning.className='parameter-error';row.append(warning);}
   const field=valueEditor(v,def,path+(array?'['+key+']':'.'+key),showHelp);heading.append(field.controls);if(remove)heading.append(remove);row.append(field.element);rows.append(row);const entry={row,name,key,field,enabled};entries.push(entry);
   function toggle(){const inactive=!!enabled&&!enabled.checked;field.element.hidden=inactive;field.controls.hidden=inactive;row.classList.toggle('parameter-unset',inactive);}entry.toggle=toggle;if(enabled){enabled.addEventListener('change',toggle);toggle();}
   if(remove)remove.addEventListener('click',()=>{entries.splice(entries.indexOf(entry),1);row.remove();reindex();});
  }
  function reindex(){if(array)entries.forEach((e,i)=>{e.key=String(i);e.name.textContent='['+i+']';e.row.setAttribute('aria-label',String(i));});}
  let creation=null;
  add.addEventListener('click',()=>{if(array){append(String(entries.length),undefined);return;}if(creation){creation.querySelector('input').focus();return;}creation=element('div');creation.className='parameter-create';const input=element('input');input.placeholder='New parameter name';input.setAttribute('aria-label','New parameter name');const save=element('button','Add'),cancel=element('button','Cancel'),error=element('span');save.type=cancel.type='button';error.className='parameter-error';function close(){creation.remove();creation=null;add.hidden=false;}function submit(){const key=input.value;if(!key.trim()||entries.some(e=>e.key===key)){error.textContent='Enter a unique, nonempty name.';input.focus();return;}append(key,undefined);close();}save.addEventListener('click',submit);cancel.addEventListener('click',close);input.addEventListener('keydown',e=>{if(e.key==='Enter'){e.preventDefault();submit();}else if(e.key==='Escape'){e.preventDefault();close();}});creation.append(input,save,cancel,error);host.append(creation);add.hidden=true;input.focus();});
  const initial=value&&typeof value==='object'?value:{};
  if(!array&&schema){for(const [k,def]of Object.entries(props))append(k,Object.hasOwn(initial,k)?initial[k]:Object.hasOwn(def,'default')?clone(def.default):undefined,true,Object.hasOwn(initial,k));}
  for(const [k,v]of Object.entries(initial))if(array||!schema||!Object.hasOwn(props,k))append(k,v);
  if(!array&&schema&&!Object.keys(props).length&&!schema.additionalProperties)host.prepend(element('p','This operator has no parameters.'));
  return{element:host,setValue(key,value){const entry=entries.find(e=>e.key===key);if(!entry)return;entry.field.setValue(value);if(entry.enabled)entry.enabled.checked=true;entry.toggle();},read:()=>{const result=array?[]:{};for(const e of entries){if(e.enabled&&!e.enabled.checked)continue;const key=e.key;if(!array&&(!key.trim()||Object.hasOwn(result,key)))throw Error('Parameter names must be nonempty and unique.');const v=e.field.read();if(array)result.push(v);else own(result,key,v);}check(schema,result,path);return result;}};
 }
 return{mount(host,value,schema){
  const description=element('section');description.id='parameter-description';description.className='parameter-description';description.setAttribute('role','status');description.setAttribute('aria-live','polite');
  const heading=element('strong','Parameter help'),text=element('p','Hover or focus a question mark to see the parameter description and constraints.');description.append(heading,text);
  const showHelp=(name,info)=>{heading.textContent=name;text.textContent=info;};
  const editor=collection(value,false,schema,'params',showHelp);host.replaceChildren(editor.element,description);return editor;
 },validate:check};
})();

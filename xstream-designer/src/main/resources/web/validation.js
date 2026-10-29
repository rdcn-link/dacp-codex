/* Fast authoring checks. This never starts workers, reads data or executes a Job. */
(function(root){
 function check(job,operators,report,variables={},validateParams=()=>{}){
  const issues=[],tasks=job.tasks||[],byId=new Map(tasks.map(t=>[t.id,t])),consumers=new Map(),parents=new Map(),reachable=new Set();
  const issue=(severity,step,code,message)=>issues.push({severity,step,code,message});
  const error=(step,code,message)=>issue('error',step,code,message);
  const parse=(value,owner)=>{if(typeof value!=='string'||!/^([A-Za-z_][\w-]*)(?::([A-Za-z_][\w-]*))?$/.test(value)){error(owner,'invalid_binding','Invalid connection: '+String(value));return null;}const [step,port='result']=value.split(':');if(!byId.has(step)){error(owner,'missing_step','Unknown source step: '+step);return null;}return{step,port};};
  function bind(value,owner,contract){const b=parse(value,owner);if(!b)return null;const key=b.step+':'+b.port;if(consumers.has(key))error(owner,'duplicate_consumer',key+' already has a consumer');else consumers.set(key,owner);
   const upstream=report?.tasks?.[b.step];if(upstream&&!upstream.diagnostics?.some(d=>d.severity==='error')){const d=upstream.outputs?.[b.port];if(!d)error(owner,'missing_output','Unknown output port: '+key);else if(contract){for(const kind of ['representations','formats'])if(contract[kind]?.length&&d[kind]?.length&&!contract[kind].some(x=>d[kind].includes(x)))error(owner,'incompatible_input',key+' has incompatible '+kind);}}
   return b.step;
  }
  function resolve(value,step){if(typeof value==='string'&&/^\$[A-Za-z_][A-Za-z0-9_]*$/.test(value)){const name=value.slice(1);if(!Object.hasOwn(variables,name)){error(step,'missing_variable','Undefined variable: '+name);return value;}return variables[name];}if(Array.isArray(value))return value.map(x=>resolve(x,step));if(value&&typeof value==='object')return Object.fromEntries(Object.entries(value).map(([k,v])=>[k,resolve(v,step)]));return value;}
  if(!tasks.length)error(null,'empty_job','Add at least one step');
  if(byId.size!==tasks.length)error(null,'duplicate_step','Step IDs must be unique');
  if(Object.keys(job.outputs||{}).length!==1)error(null,'outlet_unconnected','Outlet must connect to exactly one step output port');
  for(const t of tasks){
   const manifest=operators.find(o=>o.name+'@'+o.version===t.operator),source=t.operator==='read_item@1.0';
   if(!source&&!manifest)error(t.id,'unknown_operator','Operator not found: '+t.operator);
   const inputs=t.inputs||{},declared=source?{}:manifest?.inputs;
   if(declared){for(const p of Object.keys(declared))if(!Object.hasOwn(inputs,p))error(t.id,'missing_input','Input not connected: '+p);for(const p of Object.keys(inputs))if(!Object.hasOwn(declared,p))error(t.id,'unexpected_input','Undeclared input port: '+p);}
   parents.set(t.id,Object.entries(inputs).map(([p,v])=>bind(v,t.id,declared?.[p])).filter(Boolean));
   try{const params=resolve(t.params||{},t.id);if(source){if(typeof params.uri!=='string'||!params.uri)error(t.id,'invalid_params','read_item requires an Item URI');}else if(manifest?.parameters)validateParams(manifest.parameters,params,'params');}catch(e){error(t.id,'invalid_params',e.message);}
   const inferred=report?.tasks?.[t.id];for(const d of inferred?.diagnostics||[])issue(d.severity,t.id,d.code,d.message);
   if(!inferred)issue('warning',t.id,'inference_pending','Output structure not yet verified');
  }
  function mark(id){if(reachable.has(id))return;reachable.add(id);for(const p of parents.get(id)||[])mark(p);}
  for(const value of Object.values(job.outputs||{})){const id=bind(value,null);if(id)mark(id);}
  for(const t of tasks)if(!reachable.has(t.id))error(t.id,'disconnected_step','No dataflow path from this step to Outlet');
  const visiting=new Set(),done=new Set();function cycle(id){if(visiting.has(id)){error(id,'cycle','Dataflow contains a cycle');return;}if(done.has(id))return;visiting.add(id);for(const p of parents.get(id)||[])cycle(p);visiting.delete(id);done.add(id);}for(const t of tasks)cycle(t.id);
  return issues;
 }
 root.WorkflowValidation=Object.freeze({check});
})(globalThis);

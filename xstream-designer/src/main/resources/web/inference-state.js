/* Editing-time dependency tracking. Layout, titles and output selection do not invalidate steps. */
(function(root){
 'use strict';
 function stable(value){if(Array.isArray(value))return '['+value.map(stable).join(',')+']';if(value&&typeof value==='object')return '{'+Object.keys(value).sort().map(k=>JSON.stringify(k)+':'+stable(value[k])).join(',')+'}';return JSON.stringify(value);}
 function hash(value){let h=14695981039346656037n;for(const c of value){h^=BigInt(c.codePointAt(0));h=BigInt.asUintN(64,h*1099511628211n);}return h.toString(16);}
 function binding(value){const [task,port='result']=String(value).split(':');return {task,port};}
 function variables(value){if(typeof value==='string')return /^\$[A-Za-z_][A-Za-z0-9_]*$/.test(value);if(value&&typeof value==='object')return Object.values(value).some(variables);return false;}
 function keys(request){const tasks=JSON.parse(request.job_yaml).tasks,byId=new Map(tasks.map(t=>[t.id,t])),result={},visiting=new Set();
  function visit(id){if(Object.hasOwn(result,id))return result[id];if(visiting.has(id))throw Error('Inference graph contains a cycle');const t=byId.get(id);if(!t)return 'missing:'+id;visiting.add(id);const inputs=Object.fromEntries(Object.entries(t.inputs||{}).map(([port,value])=>{const b=binding(value);return [port,{...b,upstream:visit(b.task)}];}));const local={operator:t.operator,params:t.params||{},inputs};if(variables([t.params,t.inputs]))local.variables=request.variables_yaml;result[id]=hash(stable(local));visiting.delete(id);return result[id];}
  for(const t of tasks)visit(t.id);return result;
 }
 function outputs(report,job){if(!report)return;report.outputs={};for(const [name,value] of Object.entries(job.outputs||{})){const b=binding(value),d=report.tasks?.[b.task]?.outputs?.[b.port];if(d)report.outputs[name]=d;}}
 function retain(report,previous,next,job){if(!report)return null;const result={...report,tasks:Object.fromEntries(Object.entries(report.tasks||{}).filter(([id])=>Object.hasOwn(next,id)&&previous[id]===next[id]))};outputs(result,job);return result;}
 root.InferenceChanges={stable,keys,outputs,retain};
})(globalThis);

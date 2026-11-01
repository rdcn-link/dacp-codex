/* npm install --prefix /tmp/job-tabs-tests jsdom
 * NODE_PATH=/tmp/job-tabs-tests/node_modules node scripts/test-job-detail-tabs.cjs */
const fs=require('fs'),assert=require('assert'),{JSDOM,VirtualConsole}=require('jsdom');
const root=process.cwd(),steps=[{id:'read',title:'Read',node:'dacp://node-a:1',node_identity:'A',operator:'read_item@1.0',state:'succeeded',inputs:[]},{id:'copy',node:'dacp://node-b:2',node_identity:'B',operator:'copy@1',state:'succeeded',inputs:[{port:'source',task:'read',output_port:'result'}]}];
const job={job_id:'job-test',name:'Test',state:'succeeded',created_at_ms:1,elapsed_ms:10,submitted_job_yaml:'name: submitted',job_yaml:'name: physical',execution_policy_yaml:'submitted: true',effective_execution_policy_yaml:'effective: true',logical_plan:{steps,outputs:{}},outputs:{result:{task:'copy',port:'result'}},steps,nodes:{A:{state:'succeeded',observed_at_ms:123,history_record_id:'record-a',steps:{read:'succeeded'},execution_units:[{id:'read',mode:'host_stream_source',steps:['read']}]},B:{state:'running',observation_error:true,observed_at_ms:123,history_record_id:'record-b',execution_units:[{id:'copy',mode:'isolated_step',steps:['copy'],runtime_name:'python-arrow',runtime_version:'1.0'}]}},sub_jobs:[{node:steps[0].node,web_url:'http://node-a:80',steps:['read'],definition_yaml:'name: a'},{node:steps[1].node,web_url:'http://node-b:80',steps:['copy'],definition_yaml:'name: b'}]};
const tick=()=>new Promise(r=>setImmediate(r));
async function check(union,fromUnion=false){
 const dir=union?'dacp-union-server/src/main/resources/web':'dacp-server/src/main/resources/node-web',errors=[],vc=new VirtualConsole();vc.on('jsdomError',e=>errors.push(e));
 const dom=new JSDOM(fs.readFileSync(root+'/'+dir+'/job.html','utf8'),{url:'http://example/job?id=job-test',runScripts:'outside-only',virtualConsole:vc});const w=dom.window,d=w.document,graphs=[];
 w.DacpDag={Viewer:class{constructor(){this.layoutMode='dependency';}async update(j){graphs.push(j);}setLayout(){}}};w.DacpDuration=n=>String(n);w.DacpJobScript={format:s=>s||''};w.DacpAttempts={render(){}};
 const record={...job,record_id:'job-test',kind:fromUnion?'union_execution':'dacp_exchange',execution_source:fromUnion?'union':'client',definition_yaml:'name: received',logical_yaml:'name: logical',graph_steps:steps,graph_outputs:job.outputs,graph_nodes:job.nodes,step_records:[]};
 w.fetch=async()=>({ok:true,json:async()=>structuredClone(union?job:record)});
 w.eval(fs.readFileSync(root+'/dacp-union-server/src/main/resources/web/execution-info.js','utf8'));w.eval(fs.readFileSync(root+'/'+dir+'/job.js','utf8'));await tick();await tick();
 const click=selector=>{const e=d.querySelector(selector);assert(e,selector);e.click();};
 assert(d.querySelector('#error').hidden,d.querySelector('#error').textContent);
 assert.deepEqual([...d.querySelectorAll('.plan-tabs [data-plan]')].map(e=>e.dataset.plan),['submission','logical','physical','execution']);
 assert.equal(d.querySelectorAll('.plan-tabs .plan-tab-icon').length,4);
 for(const key of ['submission','logical','execution','physical']){click('[data-plan="'+key+'"]');await tick();assert(!d.querySelector('#'+key+'-plan').hidden);}
 assert(d.querySelector('#submitted-script').textContent.includes(union?'submitted':'received'));
 assert(d.querySelector('#execution-info').textContent.includes('python-arrow@1.0'));
 assert(d.querySelector('#execution-info').textContent.includes('showing last known state'));
 if(union){
  assert(d.querySelector('#physical-plan #subjob-plan'));assert.equal(d.querySelectorAll('#physical-plan-tabs [data-physical]').length,3);
  click('[data-physical="subjob-1"]');await tick();assert(!d.querySelector('#subjob-plan').hidden);assert(d.querySelector('#physical-overview').hidden);assert.equal(graphs.at(-1).steps.length,1);
  click('[data-plan-view="subjob:script"]');await tick();assert.equal(d.querySelector('#subjob-script').textContent,'name: b');assert(!d.querySelector('#subjob-script').hidden);
  const link=d.querySelector('#execution-info a');assert.equal(new URL(link.href).searchParams.get('id'),'record-a');
  click('[data-plan="execution"]');click('#refresh');await tick();await tick();assert(!d.querySelector('#execution-plan').hidden);
 }else if(fromUnion){click('[data-plan="logical"]');await tick();assert(!d.querySelector('#logical-unavailable').hidden);assert(d.querySelector('#plan-dag-workspace').hidden);}
 const first=d.querySelector('[data-plan="submission"]');first.focus();first.dispatchEvent(new w.KeyboardEvent('keydown',{key:'End',bubbles:true}));await tick();assert.equal(d.activeElement.dataset.plan,'execution');
 assert.deepEqual(errors,[]);dom.window.close();console.log('PASS',union?'Union':fromUnion?'Node received sub-job':'Node local job');
}
(async()=>{await check(true);await check(false);await check(false,true);})().catch(e=>{console.error(e);process.exitCode=1;});

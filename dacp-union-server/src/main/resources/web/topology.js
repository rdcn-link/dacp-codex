'use strict';
window.DacpTopology=(()=>{
 let previous=new Map(),resizeObserver;
 const colors=['#38bdf8','#fb923c','#a78bfa','#2dd4bf','#f472b6','#a3e635'];
 const el=(tag,text,cls)=>{const n=document.createElement(tag);if(text!==undefined)n.textContent=text;if(cls)n.className=cls;return n;};
 const bytes=v=>{if(v===null)return '—';const u=['B','KiB','MiB','GiB'];let i=0;while(v>=1024&&i<3){v/=1024;i++;}return v.toLocaleString('en-US',{maximumFractionDigits:1})+' '+u[i];};
 const svg=(tag,attrs)=>{const n=document.createElementNS('http://www.w3.org/2000/svg',tag);for(const [k,v]of Object.entries(attrs||{}))n.setAttribute(k,v);return n;};
 function render(data,select){
  const stage=document.querySelector('.network-stage'),lines=document.getElementById('network-lines'),nodes=document.getElementById('network-nodes');
  if(!stage)return;
  const focused=document.activeElement?.closest('.network-node')?.dataset.identity;
  const members=data.members||[],count=members.length,now=data.at_ms,windowMs=data.stale_after_ms||10000,height=count>8?230+Math.ceil(count/5)*145:560;
  let overlay=stage.querySelector('.network-labels');
  if(!overlay){overlay=svg('svg',{class:'network-labels',role:'group','aria-label':'Task traffic totals and rates'});stage.append(overlay);}
  stage.style.height=height+'px';
  for(const layer of [lines,overlay]){layer.setAttribute('viewBox','0 0 1000 '+height);layer.replaceChildren();}
  nodes.replaceChildren();
  const defs=svg('defs');lines.append(defs);const positions=new Map(),health=new Map();
  members.forEach((m,i)=>{
   const angle=-Math.PI/2+i*2*Math.PI/Math.max(count,1);
   const point=count>8?{x:100+(i%5)*200,y:220+Math.floor(i/5)*145}:{x:500+355*Math.cos(angle),y:280+190*Math.sin(angle)};
   positions.set(m.identity,point);health.set(m.identity,m.state==='approved'&&!!m.status&&!!m.observed_at_ms&&now-m.observed_at_ms<=windowMs);
  });
  const hub={x:500,y:count>8?75:280};
  const backdrop=svg('g',{'aria-hidden':'true',class:'network-orbits'});
  for(const radius of [106,158,235])backdrop.append(svg('ellipse',{cx:hub.x,cy:hub.y,rx:radius*1.48,ry:radius,class:'orbit-ring'}));
  for(let i=0;i<36;i++){const angle=i*Math.PI/18,x=hub.x+Math.cos(angle)*152,y=hub.y+Math.sin(angle)*102;backdrop.append(svg('circle',{cx:x,cy:y,r:i%3?1.5:3,class:'orbit-tick'}));}
  lines.append(backdrop);
  for(const p of positions.values())lines.append(svg('path',{d:`M ${hub.x} ${hub.y} L ${p.x} ${p.y}`,class:'membership-wire'}));
  positions.set('@union',hub);
  const edges=data.edges||[];
  const next=new Map(),labels=[];let total=0,moving=0;
  for(const [i,edge]of edges.entries()){
   const from=positions.get(edge.source),to=positions.get(edge.target);if(!from||!to)continue;
   let rate=0,knownRate=false;
   for(const sample of edge.samples||[]){
    const last=previous.get(sample.key),dt=sample.observed_at_ms-(last?.at||0),fresh=now-sample.observed_at_ms<=windowMs;
    if(sample.active&&fresh&&last&&dt>0&&sample.bytes>=last.bytes){rate+=(sample.bytes-last.bytes)*1000/dt;knownRate=true;}
    next.set(sample.key,{bytes:sample.bytes,at:sample.observed_at_ms});
   }
   const active=knownRate&&rate>0;moving+=active?1:0;if(edge.observed)total+=edge.payload_bytes;
   const color=colors[i%colors.length],mark=svg('marker',{id:'flow-arrow-'+i,viewBox:'0 0 10 10',refX:9,refY:5,markerWidth:7,markerHeight:7,orient:'auto'});mark.append(svg('path',{d:'M 0 0 L 10 5 L 0 10 z',fill:color}));defs.append(mark);
   const dx=to.x-from.x,dy=to.y-from.y,length=Math.hypot(dx,dy)||1;
   const start={x:from.x+dx/length*76,y:from.y+dy/length*55},end={x:to.x-dx/length*76,y:to.y-dy/length*55};
   const midpoint={x:(from.x+to.x)/2,y:(from.y+to.y)/2};
   let bend=75;
   if(edge.source!=='@union'&&edge.target!=='@union'&&((midpoint.x-hub.x)*(-dy)+(midpoint.y-hub.y)*dx)<0)bend=-bend;
   const control={x:midpoint.x-dy/length*bend,y:midpoint.y+dx/length*bend};
   const path=svg('path',{d:`M ${start.x} ${start.y} Q ${control.x} ${control.y} ${end.x} ${end.y}`,stroke:color,'marker-end':`url(#flow-arrow-${i})`,class:'traffic-wire'+(active?' flow-active':'')});
   const title=svg('title');title.textContent=(edge.observed?bytes(edge.payload_bytes):'Not observed')+' · '+(knownRate?bytes(rate)+'/s':'Rate unavailable');path.append(title);lines.append(path);
   const source=edge.source==='@union'?{url:'Union'}:members.find(m=>m.identity===edge.source),target=edge.target==='@union'?{url:'Union'}:members.find(m=>m.identity===edge.target);
   const totalLabel=edge.observed?bytes(edge.payload_bytes):'Not observed',rateLabel=knownRate?bytes(rate)+'/s':'Rate —';
   const description=source.url+' → '+target.url+' · '+totalLabel+' · '+rateLabel+' · Receiver-side task payload';
   title.textContent=description;
   const x=(start.x+2*control.x+end.x)/4,y=(start.y+2*control.y+end.y)/4;
   const group=svg('g',{class:'flow-label',role:'img','aria-label':description});
   group.append(svg('rect',{x:x-56,y:y-17,width:112,height:34,rx:8,class:'wire-label-bg'}));
   const totalText=svg('text',{x,y:y-3,class:'wire-label wire-total'});totalText.textContent=totalLabel;
   const rateText=svg('text',{x,y:y+10,class:'wire-label wire-rate'});rateText.textContent=rateLabel;
   group.append(totalText,rateText);const tooltip=svg('title');tooltip.textContent=description;group.append(tooltip);overlay.append(group);labels.push({group,start,control,end,x,y,color});
  }
  previous=next;
  const core=el('div',undefined,'network-core');core.style.left=hub.x/10+'%';core.style.top=hub.y/height*100+'%';core.append(el('strong','UNION'),el('span','CONTROL PLANE'));nodes.append(core);
  members.forEach((m,i)=>{
   const p=positions.get(m.identity),healthy=health.get(m.identity),button=el('button',undefined,'network-node '+(healthy?'is-healthy':'is-unknown'));button.type='button';button.dataset.identity=m.identity;button.style.left=p.x/10+'%';button.style.top=p.y/height*100+'%';button.style.setProperty('--signal',healthy?colors[i%colors.length]:'#fb7185');
   let address=m.url;try{const u=new URL(m.url);address=u.hostname+':'+u.port;}catch(_){}
   const badge=el('span',undefined,'network-node-badge');badge.append(el('span',String(i+1).padStart(2,'0'),'network-node-number'),el('span','MEMBER NODE','network-node-kind'));button.append(badge);
   button.append(el('span',healthy?'● Healthy':m.state==='approved'?'● Observation stale':'● Unknown / not approved','network-health'),el('strong',address),el('span','Tasks '+(m.status?m.status.reserved_tasks+' / '+m.status.capacity:'—')+' · Jobs '+(data.participation?.[m.identity]??'—'),'network-capacity'));
   const resource=m.status?.resources,percent=v=>v===undefined?'—':(v*100).toFixed(0)+'%';button.append(el('span','CPU '+percent(resource?.cpu_load)+' · Memory '+(resource?.memory_total_bytes?percent(resource.memory_used_bytes/resource.memory_total_bytes):'—'),'network-capacity'));const bars=el('div',undefined,'network-resource-bars');for(const [label,value]of [['CPU',resource?.cpu_load],['MEM',resource?.memory_total_bytes?resource.memory_used_bytes/resource.memory_total_bytes:undefined]]){const track=el('span',undefined,'network-resource-track');track.title=label+' '+percent(value);const fill=el('i');fill.style.width=Math.max(0,Math.min(100,(value||0)*100))+'%';track.append(fill);bars.append(track);}button.append(bars);button.title=m.url+' · '+m.identity;button.addEventListener('click',()=>select(m.identity));nodes.append(button);
  });
  // Labels have their own top layer. Slide only along the curve; never detach
  // a label from its traffic edge or put a leader through the diagram.
  function positionLabels(){
   const coreBounds=core.getBoundingClientRect(),padding=18;
   const hubBounds={left:coreBounds.left-padding,right:coreBounds.right+padding,top:coreBounds.top-padding,bottom:coreBounds.bottom+padding};
   const obstacles=[...nodes.querySelectorAll('.network-node')].map(n=>n.getBoundingClientRect()),placed=[];
   const overlaps=(a,b)=>a.left<b.right+6&&a.right>b.left-6&&a.top<b.bottom+6&&a.bottom>b.top-6;
   for(const label of labels){
    const {group,start,control,end,x,y}=label;
    let best=null;
    for(const t of [.5,.4,.6,.3,.7,.2,.8,.1,.9]){
     const u=1-t,dx=u*u*start.x+2*u*t*control.x+t*t*end.x-x,dy=u*u*start.y+2*u*t*control.y+t*t*end.y-y;
     group.setAttribute('transform',`translate(${dx} ${dy})`);
     const box=group.getBoundingClientRect();
     const score=(overlaps(box,hubBounds)?1000:0)+obstacles.filter(b=>overlaps(box,b)).length*100+placed.filter(b=>overlaps(box,b)).length*10+Math.abs(t-.5);
     if(!best||score<best.score)best={dx,dy,score};
     if(score===0)break;
    }
    group.setAttribute('transform',`translate(${best.dx} ${best.dy})`);
    const box=group.getBoundingClientRect();
    placed.push(box);
   }
  }
  positionLabels();
  resizeObserver?.disconnect();resizeObserver=new ResizeObserver(positionLabels);resizeObserver.observe(stage);
  if(focused)[...nodes.querySelectorAll('.network-node')].find(n=>n.dataset.identity===focused)?.focus({preventScroll:true});
  const healthy=[...health.values()].filter(Boolean).length;
  document.getElementById('topology-summary').textContent=healthy+' / '+count+' nodes healthy · refreshed every 5s';
  const stats=document.getElementById('network-stats');stats.replaceChildren();for(const [value,label]of [[healthy+'/'+count,'Healthy nodes'],[moving,'Active data links'],[bytes(total),'Inter-member task payload']]){const card=el('div');card.append(el('strong',String(value)),el('span',label));stats.append(card);}
  document.getElementById('traffic-scope').textContent='Recent '+(data.job_count||0)+' / 100 jobs · latest attempts only · receiver-side task payload · excludes protocol overhead, heartbeats, status queries, catalog access and other applications · rates require consecutive observations; — means unknown';
  if(!edges.length){const empty=svg('text',{x:500,y:height-15,class:'wire-label'});empty.textContent='No task traffic observed yet';lines.append(empty);}
 }
 return {render,stale(){document.getElementById('topology-summary').textContent='Observation connection lost · showing the last snapshot';document.querySelectorAll('.wire-rate').forEach(n=>n.textContent='Rate —');document.querySelectorAll('.flow-active').forEach(n=>n.classList.remove('flow-active'));document.querySelectorAll('.network-node').forEach(n=>{n.classList.remove('is-healthy');n.classList.add('is-unknown');n.style.setProperty('--signal','#fb7185');n.querySelector('.network-health').textContent='● Observation interrupted';});document.querySelectorAll('.node-health').forEach(n=>n.textContent='● Observation connection lost');}};
})();

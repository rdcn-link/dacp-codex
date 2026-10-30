'use strict';
// Pure presentation helpers. Descriptors remain plugin-owned JSON, never flattened schemas.
(function(root){
 function structureRows(s){
  if(!s||typeof s!=='object')return [];
  if(s.view==='array')return [['type',s.data_type],...(s.dimensions||[]).map(d=>[d.name,String(d.length)+(d.unit?' '+d.unit:'')])];
  if(s.view==='image')return [['type',s.data_type],['width',s.width],['height',s.height],['channels',s.channels]];
  if(s.view==='sequence')return [['alphabet',s.alphabet],['chunks','sequence ID + offset']];
  return Object.entries(s).filter(([k])=>k!=='attributes').slice(0,8).map(([k,v])=>[k,typeof v==='object'?JSON.stringify(v):String(v)]);
 }
 function preferred(views){return ['records','bytes'].find(v=>views.includes(v))||views[0];}
 function sampleColumns(rows){return [...new Set(rows.flatMap(row=>Object.keys(row)))];}
 const api={structureRows,preferred,sampleColumns};root.ScientificViewsUI=api;if(typeof module!=='undefined')module.exports=api;
})(typeof window==='undefined'?globalThis:window);

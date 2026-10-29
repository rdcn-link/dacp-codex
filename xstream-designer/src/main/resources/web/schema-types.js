/* Display names only: the original Arrow schema remains unchanged. */
(function(root){
 'use strict';
 const aliases={utf8:'string',largeutf8:'large_string',utf8view:'string_view',bool:'bool',boolean:'bool',halffloat:'float16',half:'float16',float:'float32',single:'float32',double:'float64',floatingpoint:'float64',largebinary:'large_binary',binaryview:'binary_view',fixedsizebinary:'fixed_binary',fixedsizelist:'fixed_list',largelist:'large_list',listview:'list_view',largelistview:'large_list_view'};
 const unit=u=>({second:'s',millisecond:'ms',microsecond:'us',nanosecond:'ns',day:'day',year_month:'year_month',day_time:'day_time',month_day_nano:'month_day_nano'}[String(u).toLowerCase()]||String(u??'?').toLowerCase());
 function typeName(type,children=[]){
  if(typeof type==='string')return aliases[type.toLowerCase()]||type.toLowerCase();
  if(!type||typeof type!=='object')return 'unknown';
  const name=String(type.name||'unknown').toLowerCase(),child=()=>children[0]?fieldType(children[0]):'unknown';
  switch(name){
   case 'int':return (type.isSigned===false?'uint':'int')+(type.bitWidth??'?');
   case 'floatingpoint':return {half:'float16',single:'float32',double:'float64'}[String(type.precision).toLowerCase()]||'unknown';
   case 'decimal':return `decimal${type.bitWidth||128}(${type.precision}, ${type.scale})`;
   case 'date':return String(type.unit).toLowerCase()==='day'?'date32':'date64';
   case 'time':return `time${type.bitWidth||64}(${unit(type.unit)})`;
   case 'timestamp':return `timestamp(${unit(type.unit)}${type.timezone?', '+type.timezone:''})`;
   case 'duration':case 'interval':return `${name}(${unit(type.unit)})`;
   case 'fixedsizebinary':return `fixed_binary[${type.byteWidth}]`;
   case 'list':case 'largelist':case 'listview':case 'largelistview':return `${aliases[name]||name}<${child()}>`;
   case 'fixedsizelist':return `fixed_list<${child()}>[${type.listSize}]`;
   case 'struct':return `struct<${children.map(f=>f.name+': '+fieldType(f)).join(', ')}>`;
   case 'map':{const fields=children[0]?.children||[];return `map<${fields[0]?fieldType(fields[0]):'unknown'}, ${fields[1]?fieldType(fields[1]):'unknown'}>${type.keysSorted?' [sorted]':''}`;}
   case 'union':return `union[${String(type.mode||'').toLowerCase()}]<${children.map((f,i)=>(type.typeIds?.[i]??i)+': '+f.name+': '+fieldType(f)).join(', ')}>`;
   case 'runendencoded':return `run_end_encoded<${children.map(fieldType).join(', ')}>`;
   default:return aliases[name]||name;
  }
 }
 function fieldType(field){const value=typeName(field.type,field.children||[]);return value+(field.nullable?'?':'');}
 root.SchemaTypes=Object.freeze({typeName,fieldType});
})(globalThis);

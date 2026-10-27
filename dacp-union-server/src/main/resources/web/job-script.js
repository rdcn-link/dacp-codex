'use strict';
window.DacpJobScript=(()=>{
// Preserve submitted YAML whitespace; pretty-print JSON without changing numeric or string tokens.
function format(script){
 if(!script)return 'No task script was saved for this historical record';
 try{JSON.parse(script);}catch(_){return script;}
 const tokens=script.match(/"(?:\\.|[^"\\])*"|[^\s"{}\[\],:]+|[{}\[\],:]/g)||[];
 const output=[];let depth=0;
 const newline=()=>output.push('\n'+'  '.repeat(depth));
 for(let i=0;i<tokens.length;i++){
  const token=tokens[i];
  if(token==='{'||token==='['){output.push(token);depth++;if(tokens[i+1]!=='}'&&tokens[i+1]!==']')newline();}
  else if(token==='}'||token===']'){depth--;if(tokens[i-1]!=='{'&&tokens[i-1]!=='[')newline();output.push(token);}
  else if(token===','){output.push(',');newline();}
  else if(token===':')output.push(': ');
  else output.push(token);
 }
 return output.join('');
}
return {format};
})();

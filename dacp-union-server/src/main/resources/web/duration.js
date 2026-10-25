'use strict';
window.DacpDuration=milliseconds=>{
 if(!Number.isFinite(milliseconds))return '—';
 const seconds=Math.floor(Math.max(0,milliseconds)/1000);
 return Math.floor(seconds/3600)+'h '+String(Math.floor(seconds/60)%60).padStart(2,'0')+'m '+String(seconds%60).padStart(2,'0')+'s';
};

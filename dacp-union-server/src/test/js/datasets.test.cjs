const assert = require('node:assert/strict');
const fs = require('node:fs');
const vm = require('node:vm');
const path = require('node:path');
class Element {
  constructor() { this.children = []; this.textContent = ''; this.value = ''; }
  append(...children) { this.children.push(...children); }
  replaceChildren(...children) { this.children = children; }
  setAttribute() {}
  removeAttribute() {}
  addEventListener() {}
}
const elements = new Map();
const document = {
  getElementById(id) { if (!elements.has(id)) elements.set(id, new Element()); return elements.get(id); },
  createElement() { return new Element(); }
};
let finishSlow, finishHealthy;
const response = data => ({ok: true, json: async () => data});
const context = vm.createContext({document, AbortSignal, fetch: async url => {
  if (url === '/datasets/sources') return response({sources: [
    {identity:'slow', url:'dacp://slow'}, {identity:'healthy', url:'dacp://healthy'}
  ]});
  return new Promise(resolve => {
    if (url.endsWith('/slow')) finishSlow = resolve;
    else finishHealthy = resolve;
  });
}});
const tick = () => new Promise(resolve => setImmediate(resolve));
(async () => {
  vm.runInContext(fs.readFileSync(path.join(__dirname, '../../main/resources/web/datasets.js'), 'utf8'), context);
  await tick();
  finishHealthy(response({datasets:[{name:'ds-test-good', description:'{}', uri:'dacp://union/ds-test-good'}]}));
  await tick();
  assert.match(elements.get('catalog-status').textContent, /1 datasets · 1\/2 members loaded/);
  assert.equal(elements.get('catalog-rows').children[0].children[0].textContent, 'ds-test-good');
  assert.equal(elements.get('refresh-catalog').disabled, true);
  finishSlow({ok:false, status:503});
  await tick();
  assert.match(elements.get('catalog-status').textContent, /1 datasets · 2\/2 members loaded/);
  assert.match(elements.get('catalog-error').textContent, /dacp:\/\/slow/);
  assert.equal(elements.get('catalog-rows').children[0].children[0].textContent, 'ds-test-good');
  assert.equal(elements.get('refresh-catalog').disabled, false);
  console.log('PASS: healthy catalogs render before slow members; failures retain successful rows');
})().catch(error => { console.error(error); process.exitCode = 1; });

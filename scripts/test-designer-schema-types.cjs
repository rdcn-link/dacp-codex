const assert=require('node:assert/strict');
require('../xstream-designer/src/main/resources/web/schema-types.js');
const {typeName,fieldType}=globalThis.SchemaTypes;
const cases=[
 ['utf8','string'],['DOUBLE','float64'],['bool','bool'],
 [{name:'int',bitWidth:32,isSigned:true},'int32'],
 [{name:'int',bitWidth:64,isSigned:false},'uint64'],
 [{name:'floatingpoint',precision:'SINGLE'},'float32'],
 [{name:'floatingpoint',precision:'HALF'},'float16'],
 [{name:'decimal',precision:20,scale:4,bitWidth:256},'decimal256(20, 4)'],
 [{name:'timestamp',unit:'MICROSECOND',timezone:'UTC'},'timestamp(us, UTC)'],
 [{name:'date',unit:'DAY'},'date32'],[{name:'time',unit:'MILLISECOND',bitWidth:32},'time32(ms)'],
 [{name:'duration',unit:'NANOSECOND'},'duration(ns)'],
 [{name:'fixedsizebinary',byteWidth:16},'fixed_binary[16]'],
 [{name:'largeutf8'},'large_string'],[{name:'binary'},'binary']
];
for(const [input,expected] of cases)assert.equal(typeName(input),expected);
const child={name:'value',type:{name:'utf8'},nullable:true};
assert.equal(fieldType({type:{name:'list'},children:[child],nullable:false}),'list<string?>');
assert.equal(typeName({name:'struct'},[child]),'struct<value: string?>');
assert.equal(typeName({name:'fixedsizelist',listSize:3},[child]),'fixed_list<string?>[3]');
assert.equal(typeName({name:'map'},[{children:[{type:'utf8'},{type:'int64',nullable:true}]}]),'map<string, int64?>');
assert.equal(typeName({name:'union',mode:'Dense',typeIds:[3]},[child]),'union[dense]<3: value: string?>');
assert.equal(fieldType({type:{name:'new_type'},nullable:true}),'new_type?');
console.log('Schema display names: scalars, temporal precision, nested fields, nullability and unknown types passed');

"""Common DACP operators. Pure transforms plus a bounded Arrow stream adapter."""
import base64
import csv
import gzip
import hashlib
import html
import io
import json
import re
import unicodedata
import urllib.parse
import zlib

# Each entry carries executable examples, also used as golden contract tests.
CATALOG = {}
def add(name, description, fn, sample, expected, params=None, representation='bytes', format='text'):
    assert name not in CATALOG
    CATALOG[name] = dict(description=description, fn=fn, sample=sample, expected=expected,
                         params=params or {}, representation=representation, format=format)


def text(name, description, fn, sample, expected, params=None):
    add('text_'+name, description, lambda b,p: fn(b.decode('utf-8'),p).encode('utf-8'),
        sample.encode(), expected.encode(), params)

text('uppercase','Unicode uppercase / Unicode 转大写',lambda s,p:s.upper(),'Hello 世界','HELLO 世界')
text('lowercase','Unicode lowercase / Unicode 转小写',lambda s,p:s.lower(),'Hello','hello')
text('casefold','Unicode case folding / Unicode 无大小写规范化',lambda s,p:s.casefold(),'Straße','strasse')
text('titlecase','Title case / 单词首字母大写',lambda s,p:s.title(),'hello world','Hello World')
text('capitalize','Capitalize sentence / 句首大写',lambda s,p:s.capitalize(),'hELLO','Hello')
text('swapcase','Swap letter case / 大小写互换',lambda s,p:s.swapcase(),'AbC','aBc')
text('strip','Trim surrounding whitespace / 去除首尾空白',lambda s,p:s.strip(),' a \n','a')
text('lstrip','Trim leading whitespace / 去除开头空白',lambda s,p:s.lstrip(),' a ','a ')
text('rstrip','Trim trailing whitespace / 去除结尾空白',lambda s,p:s.rstrip(),' a ',' a')
text('normalize_spaces','Collapse whitespace / 合并连续空白',lambda s,p:' '.join(s.split()),'a  b\nc','a b c')
text('normalize_newlines','Normalize CRLF and CR / 统一换行符',lambda s,p:s.replace('\r\n','\n').replace('\r','\n'),'a\r\nb\r','a\nb\n')
text('remove_blank_lines','Remove blank lines / 删除空白行',lambda s,p:'\n'.join(x for x in s.splitlines() if x.strip()),'a\n \nb','a\nb')
text('trim_lines','Trim each line / 去除各行首尾空白',lambda s,p:'\n'.join(x.strip() for x in s.splitlines()),' a \n b ','a\nb')
text('sort_lines','Sort lines lexically / 按字典顺序排列各行',lambda s,p:'\n'.join(sorted(s.splitlines())),'b\na','a\nb')
text('reverse_lines','Reverse line order / 反转行顺序',lambda s,p:'\n'.join(reversed(s.splitlines())),'a\nb','b\na')
text('unique_lines','Deduplicate lines preserving order / 保序去重行',lambda s,p:'\n'.join(dict.fromkeys(s.splitlines())),'a\nb\na','a\nb')
text('number_lines','Number lines starting at one / 从一开始添加行号',lambda s,p:'\n'.join(f'{i}\t{x}' for i,x in enumerate(s.splitlines(),1)),'a\nb','1\ta\n2\tb')
text('head_lines','First n lines / 提取前 n 行',lambda s,p:'\n'.join(s.splitlines()[:count(p)]),'a\nb\nc','a\nb',{'n':2})
text('tail_lines','Last n lines / 提取后 n 行',lambda s,p:'\n'.join(s.splitlines()[-count(p):]) if count(p) else '', 'a\nb\nc','b\nc',{'n':2})
text('filter_lines','Keep lines containing literal text / 保留包含指定文本的行',lambda s,p:'\n'.join(x for x in s.splitlines() if p['text'] in x),'apple\npear','apple',{'text':'app'})
text('exclude_lines','Exclude lines containing literal text / 排除包含指定文本的行',lambda s,p:'\n'.join(x for x in s.splitlines() if p['text'] not in x),'apple\npear','pear',{'text':'app'})
text('replace','Replace literal substring / 替换文本片段',lambda s,p:s.replace(p['old'],p['new']),'a,b','a;b',{'old':',','new':';'})
text('prefix_lines','Prefix each line / 各行添加前缀',lambda s,p:'\n'.join(p['prefix']+x for x in s.splitlines()),'a\nb','> a\n> b',{'prefix':'> '})
text('suffix_lines','Suffix each line / 各行添加后缀',lambda s,p:'\n'.join(x+p['suffix'] for x in s.splitlines()),'a\nb','a!\nb!',{'suffix':'!'})
text('normalize_nfc','Unicode NFC normalization / Unicode NFC 规范化',lambda s,p:unicodedata.normalize('NFC',s),'e\u0301','é')
text('normalize_nfkc','Unicode NFKC normalization / Unicode NFKC 规范化',lambda s,p:unicodedata.normalize('NFKC',s),'ＡＢ','AB')
text('html_escape','Escape HTML text / HTML 文本转义',lambda s,p:html.escape(s),'<a>','&lt;a&gt;')
text('html_unescape','Decode HTML entities / 还原 HTML 实体',lambda s,p:html.unescape(s),'&amp;','&')
text('url_encode','Percent encode UTF-8 / URL 百分号编码',lambda s,p:urllib.parse.quote(s,safe=''),'a b/','a%20b%2F')
text('url_decode','Percent decode UTF-8 / URL 百分号解码',lambda s,p:urllib.parse.unquote(s,errors='strict'),'a%20b','a b')

def count(p):
    n=p['n']
    if not isinstance(n,int) or isinstance(n,bool) or n<0: raise ValueError('n must be a nonnegative integer')
    return n

def dump(v): return json.dumps(v,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode()
def obj(b): return json.loads(b)
def jsonop(name,description,fn,sample,expected,params=None):
    add('json_'+name,description,lambda b,p:dump(fn(obj(b),p)),dump(sample),dump(expected),params,format='json')

jsonop('identity','Validate and compact JSON / 校验并压缩 JSON',lambda v,p:v,{'a':1},{'a':1})
add('json_pretty','Pretty-print JSON / JSON 缩进排版',lambda b,p:json.dumps(obj(b),ensure_ascii=False,indent=2,allow_nan=False).encode(),b'{"a":1}',b'{\n  "a": 1\n}',format='json')
add('json_sort_keys','Sort object keys recursively / 递归排序对象键',lambda b,p:json.dumps(obj(b),sort_keys=True,ensure_ascii=False,separators=(',',':'),allow_nan=False).encode(),b'{"b":2,"a":1}',b'{"a":1,"b":2}',format='json')
jsonop('keys','List object keys / 列出对象键',lambda v,p:list(v.keys()),{'a':1},['a'])
jsonop('values','List object values / 列出对象值',lambda v,p:list(v.values()),{'a':1},[1])
jsonop('entries','Convert object to key-value pairs / 对象转键值对',lambda v,p:list(map(list,v.items())),{'a':1},[['a',1]])
jsonop('get','Get top-level key / 提取顶层键值',lambda v,p:v[p['key']],{'a':1},1,{'key':'a'})
jsonop('select','Select object keys / 选择对象字段',lambda v,p:{k:v[k] for k in p['keys']},{'a':1,'b':2},{'a':1},{'keys':['a']})
jsonop('drop','Drop object keys / 删除对象字段',lambda v,p:{k:x for k,x in v.items() if k not in p['keys']},{'a':1,'b':2},{'b':2},{'keys':['a']})
jsonop('rename','Rename object key / 重命名对象字段',lambda v,p:{p['new'] if k==p['old'] else k:x for k,x in v.items()},{'a':1},{'b':1},{'old':'a','new':'b'})
jsonop('set','Set object key / 设置对象字段',lambda v,p:{**v,p['key']:p['value']},{'a':1},{'a':1,'b':2},{'key':'b','value':2})
jsonop('array_length','Count array elements / 统计数组长度',lambda v,p:len(v),[1,2],2)
jsonop('array_reverse','Reverse array / 反转数组',lambda v,p:list(reversed(v)),[1,2],[2,1])
jsonop('array_sort','Sort comparable array elements / 排序同类数组元素',lambda v,p:sorted(v),[2,1],[1,2])
jsonop('array_unique','Deduplicate JSON array / 保序去重 JSON 数组',lambda v,p:list({dump(x):x for x in v}.values()),[1,1,2],[1,2])
jsonop('array_head','First n array elements / 提取数组前 n 项',lambda v,p:v[:count(p)],[1,2,3],[1,2],{'n':2})
jsonop('array_flatten','Flatten one array level / 展平一层数组',lambda v,p:[x for a in v for x in a],[[1],[2,3]],[1,2,3])
jsonop('pluck','Extract key from object array / 提取对象数组的字段',lambda v,p:[x[p['key']] for x in v],[{'a':1},{'a':2}],[1,2],{'key':'a'})
jsonop('filter_equal','Filter object array by equality / 按字段相等筛选对象数组',lambda v,p:[x for x in v if x[p['key']]==p['value']],[{'a':1},{'a':2}],[{'a':2}],{'key':'a','value':2})
add('json_to_ndjson','JSON array to NDJSON / JSON 数组转 NDJSON',lambda b,p:b''.join(dump(x)+b'\n' for x in obj(b)),b'[{"a":1},{"a":2}]',b'{"a":1}\n{"a":2}\n',format='ndjson')

# CSV operations use UTF-8 with a required, unique header. Output uses LF.
def csv_read(b):
    rows=list(csv.reader(io.StringIO(b.decode('utf-8'),newline='')))
    if not rows or len(set(rows[0]))!=len(rows[0]): raise ValueError('CSV requires a unique header')
    if any(len(r)!=len(rows[0]) for r in rows[1:]): raise ValueError('CSV row width differs from header')
    return rows[0],rows[1:]
def csv_write(h,rows):
    out=io.StringIO(newline='');w=csv.writer(out,lineterminator='\n');w.writerow(h);w.writerows(rows);return out.getvalue().encode()
def csvop(name,description,fn,expected,params=None,sample=b'name,value\nb,2\na,1\nb,2\n'):
    def run(b,p):
        h,r=csv_read(b);a,z=fn(h,r,p);return csv_write(a,z)
    add('csv_'+name,description,run,sample,expected.encode(),params,format='csv')
def cols(h,p):return [h.index(k) for k in p['columns']]
csvop('identity','Validate and normalize CSV / 校验并规范 CSV',lambda h,r,p:(h,r),'name,value\nb,2\na,1\nb,2\n')
csvop('select','Select CSV columns / 选择 CSV 列',lambda h,r,p:(p['columns'],[[x[i] for i in cols(h,p)] for x in r]),'name\nb\na\nb\n',{'columns':['name']})
csvop('drop','Drop CSV columns / 删除 CSV 列',lambda h,r,p:([x for x in h if x not in p['columns']],[[v for k,v in zip(h,x) if k not in p['columns']] for x in r]),'value\n2\n1\n2\n',{'columns':['name']})
csvop('rename','Rename CSV column / 重命名 CSV 列',lambda h,r,p:([p['new'] if x==p['old'] else x for x in h],r),'label,value\nb,2\na,1\nb,2\n',{'old':'name','new':'label'})
csvop('head','First n CSV rows / 提取前 n 条 CSV 记录',lambda h,r,p:(h,r[:count(p)]),'name,value\nb,2\n',{'n':1})
csvop('tail','Last n CSV rows / 提取后 n 条 CSV 记录',lambda h,r,p:(h,r[-count(p):] if count(p) else []),'name,value\nb,2\n',{'n':1})
csvop('reverse','Reverse CSV row order / 反转 CSV 记录顺序',lambda h,r,p:(h,list(reversed(r))),'name,value\nb,2\na,1\nb,2\n')
csvop('unique','Deduplicate CSV rows / 保序去重 CSV 记录',lambda h,r,p:(h,list(dict.fromkeys(tuple(x) for x in r))),'name,value\nb,2\na,1\n')
csvop('sort','Sort by text column / 按文本列排序',lambda h,r,p:(h,sorted(r,key=lambda x:x[h.index(p['column'])])),'name,value\na,1\nb,2\nb,2\n',{'column':'name'})
csvop('sort_numeric','Sort by numeric column / 按数值列排序',lambda h,r,p:(h,sorted(r,key=lambda x:float(x[h.index(p['column'])]))),'name,value\na,1\nb,2\nb,2\n',{'column':'value'})
csvop('filter_equal','Keep matching CSV rows / 保留字段匹配的 CSV 记录',lambda h,r,p:(h,[x for x in r if x[h.index(p['column'])]==p['value']]),'name,value\na,1\n',{'column':'name','value':'a'})
csvop('filter_not_equal','Exclude matching CSV rows / 排除字段匹配的 CSV 记录',lambda h,r,p:(h,[x for x in r if x[h.index(p['column'])]!=p['value']]),'name,value\nb,2\nb,2\n',{'column':'name','value':'a'})
csvop('filter_contains','Filter CSV substring / 按子串筛选 CSV 记录',lambda h,r,p:(h,[x for x in r if p['value'] in x[h.index(p['column'])]]),'name,value\na,1\n',{'column':'name','value':'a'})
csvop('trim','Trim all CSV cells / 去除 CSV 单元格首尾空白',lambda h,r,p:(h,[[x.strip() for x in row] for row in r]),'name,value\nb,2\na,1\nb,2\n')
csvop('uppercase','Uppercase CSV column / CSV 指定列转大写',lambda h,r,p:(h,[[v.upper() if i==h.index(p['column']) else v for i,v in enumerate(row)] for row in r]),'name,value\nB,2\nA,1\nB,2\n',{'column':'name'})
csvop('lowercase','Lowercase CSV column / CSV 指定列转小写',lambda h,r,p:(h,[[v.lower() if i==h.index(p['column']) else v for i,v in enumerate(row)] for row in r]),'name,value\nb,2\na,1\nb,2\n',{'column':'name'})
csvop('fill_empty','Fill empty CSV cells / 填充 CSV 空单元格',lambda h,r,p:(h,[[v if v else p['value'] for v in row] for row in r]),'name,value\na,NA\n',{'value':'NA'},b'name,value\na,\n')
csvop('add_constant','Add constant CSV column / 添加常量 CSV 列',lambda h,r,p:(h+[p['column']],[x+[p['value']] for x in r]),'name,value,region\nb,2,east\na,1,east\nb,2,east\n',{'column':'region','value':'east'})
add('csv_to_ndjson','CSV to JSON objects / CSV 转 NDJSON 对象',lambda b,p:b''.join(dump(dict(zip(csv_read(b)[0],r)))+b'\n' for r in csv_read(b)[1]),b'a,b\n1,2\n',b'{"a":"1","b":"2"}\n',format='ndjson')
add('csv_count','Count CSV data rows / 统计 CSV 数据行数',lambda b,p:dump({'rows':len(csv_read(b)[1])}),b'a\nx\ny\n',b'{"rows":2}',format='json')

# Record transforms operate on individual Arrow batches, preserving streaming.
def record(name,description,fn,expected,params=None,sample=None):
    add('records_'+name,description,fn,sample or {'name':['b','a','b'],'value':[2,1,2]},expected,params,'records','arrow')
record('identity','Pass Arrow record batches / 透传 Arrow 记录批',lambda t,p:t,{'name':['b','a','b'],'value':[2,1,2]})
record('select','Select Arrow columns / 选择 Arrow 列',lambda t,p:t.select(p['columns']),{'name':['b','a','b']},{'columns':['name']})
record('drop','Drop Arrow columns / 删除 Arrow 列',lambda t,p:t.drop(p['columns']),{'value':[2,1,2]},{'columns':['name']})
record('rename','Rename Arrow column / 重命名 Arrow 列',lambda t,p:t.rename_columns([p['new'] if x==p['old'] else x for x in t.column_names]),{'label':['b','a','b'],'value':[2,1,2]},{'old':'name','new':'label'})
record('reverse_columns','Reverse column order / 反转列顺序',lambda t,p:t.select(list(reversed(t.column_names))),{'value':[2,1,2],'name':['b','a','b']})
record('sort_columns','Sort columns by name / 按列名排序',lambda t,p:t.select(sorted(t.column_names)),{'name':['b','a','b'],'value':[2,1,2]})
record('filter_equal','Filter records by equality / 按字段相等过滤记录',lambda t,p:t.filter(pc.equal(t[p['column']],p['value'])),{'name':['a'],'value':[1]},{'column':'name','value':'a'})
record('filter_not_equal','Filter records by inequality / 按字段不等过滤记录',lambda t,p:t.filter(pc.not_equal(t[p['column']],p['value'])),{'name':['b','b'],'value':[2,2]},{'column':'name','value':'a'})
record('filter_greater','Filter records above threshold / 过滤大于阈值的记录',lambda t,p:t.filter(pc.greater(t[p['column']],p['value'])),{'name':['b','b'],'value':[2,2]},{'column':'value','value':1})
record('filter_less','Filter records below threshold / 过滤小于阈值的记录',lambda t,p:t.filter(pc.less(t[p['column']],p['value'])),{'name':['a'],'value':[1]},{'column':'value','value':2})
record('filter_nonnull','Keep nonnull records / 保留字段非空的记录',lambda t,p:t.filter(pc.is_valid(t[p['column']])),{'value':[1]},{'column':'value'},{'value':[1,None]})
record('filter_null','Keep null records / 保留字段为空的记录',lambda t,p:t.filter(pc.is_null(t[p['column']])),{'value':[None]},{'column':'value'},{'value':[1,None]})
def change(t,p,fn):return t.set_column(t.schema.get_field_index(p['column']),p['column'],fn(t[p['column']]))
record('uppercase','Uppercase Arrow string column / Arrow 字符串列转大写',lambda t,p:change(t,p,pc.utf8_upper),{'name':['B','A','B'],'value':[2,1,2]},{'column':'name'})
record('lowercase','Lowercase Arrow string column / Arrow 字符串列转小写',lambda t,p:change(t,p,pc.utf8_lower),{'name':['b','a','b'],'value':[2,1,2]},{'column':'name'})
record('trim','Trim Arrow string column / 去除 Arrow 字符串列首尾空白',lambda t,p:change(t,p,pc.utf8_trim_whitespace),{'name':['b','a','b'],'value':[2,1,2]},{'column':'name'})
record('fill_null','Fill null values / 填充空值',lambda t,p:change(t,p,lambda a:pc.fill_null(a,p['value'])),{'value':[1,0]},{'column':'value','value':0},{'value':[1,None]})
record('add','Add scalar to numeric column / 数值列加常量',lambda t,p:change(t,p,lambda a:pc.add(a,p['value'])),{'name':['b','a','b'],'value':[3,2,3]},{'column':'value','value':1})
record('multiply','Multiply numeric column by scalar / 数值列乘常量',lambda t,p:change(t,p,lambda a:pc.multiply(a,p['value'])),{'name':['b','a','b'],'value':[4,2,4]},{'column':'value','value':2})
record('absolute','Absolute value of numeric column / 数值列取绝对值',lambda t,p:change(t,p,pc.abs),{'value':[2,1]},{'column':'value'},{'value':[-2,1]})
record('negate','Negate numeric column / 数值列取相反数',lambda t,p:change(t,p,pc.negate),{'name':['b','a','b'],'value':[-2,-1,-2]},{'column':'value'})

for algorithm in ('sha256','sha512','sha3_256','blake2b'):
    add('bytes_'+algorithm,'Compute '+algorithm+' digest / 计算数据摘要',lambda b,p,a=algorithm:hashlib.new(a,b).hexdigest().encode()+b'\n',b'abc',hashlib.new(algorithm,b'abc').hexdigest().encode()+b'\n')
add('bytes_base64_encode','Base64 encode / Base64 编码',lambda b,p:base64.b64encode(b),b'abc',b'YWJj')
add('bytes_base64_decode','Strict Base64 decode / 严格 Base64 解码',lambda b,p:base64.b64decode(b,validate=True),b'YWJj',b'abc')
add('bytes_hex_encode','Hexadecimal encode / 十六进制编码',lambda b,p:b.hex().encode(),b'abc',b'616263')
add('bytes_hex_decode','Hexadecimal decode / 十六进制解码',lambda b,p:bytes.fromhex(b.decode('ascii')),b'616263',b'abc',format='binary')
add('bytes_gzip_compress','Deterministic GZIP compression / 可复现 GZIP 压缩',lambda b,p:gzip.compress(b,mtime=0),b'abc',gzip.compress(b'abc',mtime=0),format='binary')
add('bytes_count','Count input bytes / 统计输入字节数',lambda b,p:dump({'bytes':len(b)}),b'abc',b'{"bytes":3}',format='json')

import pyarrow as pa
import pyarrow.compute as pc

MAX_INPUT = 16 * 1024 * 1024

def execute(name, ctx):
    spec=CATALOG[name]
    params=ctx.params.copy()
    limit=params.pop('max_input_bytes',MAX_INPUT)
    if not isinstance(limit,int) or isinstance(limit,bool) or not 0<limit<=MAX_INPUT:
        raise ValueError('max_input_bytes must be 1..16777216')
    if set(params)!=set(spec['params']):
        raise ValueError('expected parameters: '+', '.join(spec['params']))
    source=ctx.input('source')
    if source.representation != spec['representation']: raise ValueError('input representation mismatch')
    if spec['representation']=='records':
        schema=spec['fn'](pa.Table.from_batches([],schema=source.schema),params).schema
        output=ctx.records_output('result',schema)
        for batch in source.records():
            for result in spec['fn'](pa.Table.from_batches([batch]),params).to_batches():output.write(result)
    else:
        output=ctx.bytes_output('result',spec['format'])
        data=bytearray()
        for chunk in source.bytes():
            if len(data)+len(chunk)>limit:raise ValueError('document exceeds max_input_bytes; use smaller items')
            data.extend(chunk)
        output.write(spec['fn'](bytes(data),params))


def infer_outputs(name, inputs, params):
    from dacp_operator import descriptor, result
    spec = CATALOG[name]
    if set(inputs) != {'source'} or (inputs['source']['representations'] and inputs['source']['representations'] != [spec['representation']]):
        raise ValueError('unexpected input representation')
    if set(params) - {'max_input_bytes'} != set(spec['params']):
        raise ValueError('unexpected or missing parameters')
    return result({'result': descriptor([spec['representation']], [spec['format']], certainty='known' if spec['representation']=='bytes' else 'unknown')})


def parameter_schema(example):
    """Build-time declarations for the fixed, tested common-operator parameter examples."""
    if isinstance(example, dict):
        return dict(type='object', properties={k: parameter_schema(v) for k, v in example.items()},
                    required=list(example), additionalProperties=False)
    if isinstance(example, list):
        return dict(type='array', items=parameter_schema(example[0]) if example else dict(type='string'))
    return dict(type='null' if example is None else 'boolean' if isinstance(example, bool) else
                'integer' if isinstance(example, int) else 'number' if isinstance(example, float) else 'string')

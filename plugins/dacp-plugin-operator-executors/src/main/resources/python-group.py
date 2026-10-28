"""One interpreter, explicit process(ctx) callables, bounded in-memory Arrow links."""
import base64
from concurrent.futures import ThreadPoolExecutor
import importlib.util
import io
import json
import os
from pathlib import Path
import queue
import struct
import subprocess
import sys
import tempfile
import threading
import time
import pyarrow as pa


def main(directory):
    directory=Path(directory)
    sdk=importlib.util.spec_from_file_location('dacp_operator',directory/'group-sdk.py',submodule_search_locations=[str(directory)])
    module=importlib.util.module_from_spec(sdk);sys.modules['dacp_operator']=module;sdk.loader.exec_module(module)
    spec=json.loads((directory/'group.json').read_text())
    external={x['port']:x for x in json.loads((directory/'manifest.json').read_text())}
    failed=threading.Event();failure=[];lock=threading.RLock();threads=[]
    stdout=sys.stdout.buffer
    def active():
        if failed.is_set():raise RuntimeError('group failed') from failure[0]
    def fail(exc):
        with lock:
            if not failure:failure.append(exc)
            failed.set()
    def emit(header,payload=b''):
        h=json.dumps(header).encode()
        with lock:
            stdout.write(struct.pack('>II',len(h),len(payload))+h+payload);stdout.flush()
    record_ports=set()
    def observe(task,direction,port,h,payload):
        n=dict(type='task_metric',task=task,direction=direction,port=port,phase=h['type'],payload_bytes=len(payload))
        key=(task,direction,port)
        if 'representation' in h:
            n['representation']=h['representation']
            if h['representation']=='records':record_ports.add(key)
        if h['type']=='data' and key in record_ports:
            with pa.ipc.open_stream(payload) as reader:n['records']=sum(b.num_rows for b in reader)
        emit(n)
    def validate(contract,h):
        if h['representation'] not in contract['representations'] or (contract['formats'] and h['format'] not in contract['formats']):raise ValueError('group port contract mismatch')
        if 'schema' in contract and pa.ipc.read_schema(pa.BufferReader(base64.b64decode(contract['schema'])))!=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(h['schema']))):raise ValueError('schema mismatch')
    class Channel(io.RawIOBase):
        def __init__(self):super().__init__();self.q=queue.Queue(4);self.pending=b'';self.eof=False
        def readable(self):return True
        def writable(self):return True
        def seekable(self):return False
        def write(self,data):
            b=bytes(data)
            for pos in range(0,len(b),65536):
                while True:
                    active()
                    try:self.q.put(b[pos:pos+65536],timeout=.1);break
                    except queue.Full:pass
            return len(b)
        def finish(self):
            while True:
                active()
                try:self.q.put(None,timeout=.1);return
                except queue.Full:pass
        def read(self,n=-1):
            if n<0:raise ValueError('unbounded channel read forbidden')
            result=bytearray()
            while len(result)<n:
                active()
                if not self.pending and not self.eof:
                    try:
                        value=self.q.get(timeout=.1)
                        if value is None:self.eof=True
                        else:self.pending=value
                    except queue.Empty:continue
                if self.eof and not self.pending:break
                take=min(n-len(result),len(self.pending));result.extend(self.pending[:take]);self.pending=self.pending[take:]
            return bytes(result)
        def readinto(self,b):
            data=self.read(len(b));b[:len(data)]=data;return len(data)
    class Link:
        def __init__(self,task,port,contract):self.task,self.port,self.contract=task,port,contract;self.channel=Channel();self.meta=None;self.ready=threading.Event();self.events=queue.Queue(2)
        def send(self,h,payload):
            if h['type']=='start':
                validate(self.contract,h)
                if self.ready.is_set():raise ValueError('duplicate internal metadata')
                self.meta=h;self.ready.set()
            observe(self.task,'inputs',self.port,h,payload)
            while True:
                active()
                try:self.events.put((h,payload),timeout=.1);return
                except queue.Full:pass
        def next(self):
            while True:
                active()
                try:return self.events.get(timeout=.1)
                except queue.Empty:pass
        def run(self):
            try:
                h,_=self.next();records=h['representation']=='records'
                schema=pa.ipc.read_schema(pa.BufferReader(base64.b64decode(h['schema']))) if records else pa.schema([('data',pa.binary())])
                with pa.ipc.new_stream(self.channel,schema) as writer:
                    while True:
                        h,payload=self.next()
                        if h['type']=='end':break
                        if h['type']!='data':raise ValueError('invalid internal event')
                        if records:
                            with pa.ipc.open_stream(payload) as reader:
                                if reader.schema!=schema:raise ValueError('schema changed')
                                batches=list(reader)
                                if len(batches)!=1:raise ValueError('expected one record batch')
                                writer.write_batch(batches[0])
                        else:writer.write_batch(pa.record_batch([pa.array([payload],type=pa.binary())],schema=schema))
                self.channel.finish()
            except BaseException as e:fail(e)
    artifact=Path(spec['steps'][0]['artifact'])
    if artifact.suffix=='.zip':
        sys.path.insert(0,str(artifact))
    elif artifact.suffix=='.whl':
        package=tempfile.mkdtemp(prefix='dacp-group-package-')
        subprocess.run([sys.executable,'-m','pip','install','--no-index','--no-deps','--no-compile','--target',package,str(artifact)],check=True,stdout=sys.stderr)
        sys.path.insert(0,package)
    else:sys.path.insert(0,str(artifact.parent))
    # Preflight all entrypoints before consuming external inputs.
    functions={}
    for step in spec['steps']:
        if artifact.suffix in ('.whl','.zip'):m=__import__(step['entrypoint'],fromlist=['process'])
        else:
            ms=importlib.util.spec_from_file_location('dacp_group_'+step['id'],artifact);m=importlib.util.module_from_spec(ms);ms.loader.exec_module(m)
        fn=getattr(m,'process',None)
        if not callable(fn):raise ValueError('Python group requires process(ctx)')
        functions[step['id']]=fn
    links={};targets={}
    for step in spec['steps']:
        for port,b in step['bindings'].items():
            if 'task' in b:
                key=(b['task'],b['port'])
                if key in links:raise ValueError('fan-out unsupported')
                link=Link(step['id'],port,step['inputs'][port]);links[key]=link;targets[(step['id'],port)]=link
    selected={(b['task'],b['port']):p for p,b in spec['outputs'].items()};metadata=set();metadata_ready=threading.Event()
    def execute(step,work):
        task=step['id'];d=Path(work)/task;d.mkdir();manifest=[];streams={}
        try:
            for port,b in sorted(step['bindings'].items()):
                if 'task' in b:
                    link=targets[(task,port)]
                    while not link.ready.wait(.1):active()
                    h=link.meta;stream=link.channel
                else:
                    h=external[b['port']];stream=(directory/h['file']).open('rb');validate(step['inputs'][port],h)
                manifest.append(dict(h,port=port,file=port+'.arrow'));streams[port]=stream
            (d/'manifest.json').write_text(json.dumps(manifest));(d/'params.json').write_text(json.dumps(step['params']))
            begun=set();ended=set();ports=set(step['outputs'])
            def output(h,payload):
                active();port=h['port'];key=(task,port)
                kind=h['type']
                if kind=='start':
                    if port in begun:raise ValueError('duplicate output')
                    validate(step['outputs'].get(port,dict(representations=['bytes','records'],formats=[])),h);begun.add(port)
                elif kind=='data':
                    if port not in begun or port in ended:raise ValueError('invalid output data')
                elif kind=='end':
                    if port not in begun or port in ended:raise ValueError('invalid output end')
                    ended.add(port)
                else:raise ValueError('invalid output event')
                observe(task,'outputs',port,h,payload)
                if key in links:links[key].send(h,payload)
                if key in selected:
                    if kind!='start':
                        while not metadata_ready.wait(.1):active()
                    emit(dict(h,port=selected[key]),payload)
                    if kind=='start':
                        with lock:
                            metadata.add(key)
                            if len(metadata)==len(selected):metadata_ready.set()
            emit(dict(type='task_state',task=task,state='running',runtime_pid=os.getpid()))
            with module.OperatorContext(['operator',str(d),str(d/'params.json')],input_streams=streams,emit=output) as ctx:functions[task](ctx)
            if ended!=begun or not ports.issubset(ended):raise ValueError('incomplete output')
            emit(dict(type='task_state',task=task,state='succeeded',runtime_pid=os.getpid()))
        except BaseException as e:fail(e);raise
        finally:
            for stream in streams.values():stream.close()
    with tempfile.TemporaryDirectory(prefix='dacp-group-') as work:
        for link in links.values():
            t=threading.Thread(target=link.run,daemon=True);threads.append(t);t.start()
        pool=ThreadPoolExecutor(max_workers=len(spec['steps']))
        futures=[pool.submit(execute,s,work) for s in spec['steps']]
        try:
            while any(not f.done() for f in futures):active();time.sleep(.02)
            for f in futures:f.result()
            active()
        finally:
            pool.shutdown(wait=not failed.is_set(),cancel_futures=True)
            for t in threads:t.join(2)
    if failure:raise RuntimeError('group failed') from failure[0]

if __name__=='__main__':
    try:main(sys.argv[1])
    except BaseException:
        import traceback
        traceback.print_exc(file=sys.stderr);sys.stderr.flush()
        os._exit(1)

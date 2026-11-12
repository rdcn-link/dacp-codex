#!/usr/bin/env python3
"""CSV filtering: server-side DACP vs streaming client-side FTP/FTPS."""
import argparse
from contextlib import closing
import ftplib
import hashlib
import json
import os
import psutil
from pathlib import Path
import platform
import random
import ssl
import sys
import time
from types import SimpleNamespace
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as csv
from suite import Servers, Resources, METHODS, ROOT, write_json, runtime_inventory
from dacp import Source
from dacp.expressions import col

NAMES=['id','bucket','quality','payload']
SCHEMA=pa.schema([(n,pa.int64() if n!='payload' else pa.string()) for n in NAMES])
THRESHOLDS=[10,100,1000,5000,10000]

def fingerprint(batch, digests):
    # Separate canonical CSV column streams make the digest independent of batch boundaries.
    for index,digest in enumerate(digests):
        sink=pa.BufferOutputStream()
        csv.write_csv(pa.table([batch.column(index)],names=[NAMES[index]]),sink,
                      write_options=csv.WriteOptions(include_header=False))
        digest.update(sink.getvalue())

def generate(path,rows):
    digests={t:[hashlib.sha256() for _ in NAMES] for t in THRESHOLDS}
    counts={t:0 for t in THRESHOLDS}
    source_hash=hashlib.sha256()
    with path.open('wb') as out:
        header=(','.join(NAMES)+'\n').encode();out.write(header);source_hash.update(header)
        for start in range(0,rows,10000):
            values=[[str(i),f'{i%10000:04d}','0',hashlib.sha256(str(i).encode()).hexdigest()*2]
                    for i in range(start,min(rows,start+10000))]
            block=(''.join(','.join(v)+'\n' for v in values)).encode()
            out.write(block);source_hash.update(block)
            batch=pa.RecordBatch.from_pylist([dict(zip(NAMES,[int(v[0]),int(v[1]),int(v[2]),v[3]])) for v in values],schema=SCHEMA)
            for t in THRESHOLDS:
                selected=batch.filter(pc.less_equal(batch.column(1),t-1))
                counts[t]+=selected.num_rows;fingerprint(selected,digests[t])
    return dict(rows=rows,bytes=path.stat().st_size,sha256=source_hash.hexdigest(),
                expected={str(t):dict(rows=counts[t],sha256=[d.hexdigest() for d in digests[t]]) for t in THRESHOLDS})

class Client:
    def __init__(self,method,servers):
        self.method=method;self.servers=servers
        host=getattr(servers,'host','127.0.0.1')
        ca=getattr(servers,'ca',servers.out/'tls/cert.pem')
        if method=='dacp-tls':
            self.source=Source.open(f'dacp://{host}:{servers.ports[method]}/bench',
                 tls_root_certs=ca.read_bytes())
            try:self.source._client.wait_for_available(timeout=30)
            except BaseException:self.source.close();raise
        else:
            self.ftp=(ftplib.FTP_TLS(context=ssl.create_default_context(cafile=str(ca)))
                      if method=='ftps' else ftplib.FTP())
            self.ftp.connect(host,servers.ports[method],timeout=300)
            self.ftp.login('anonymous','benchmark@example.invalid')
            if method=='ftps':self.ftp.prot_p()
            self.ftp.voidcmd('TYPE I')
    def close(self):
        if self.method=='dacp-tls':self.source.close()
        else:self.ftp.close()
    def batches(self,path,threshold):
        if self.method=='dacp-tls':
            with self.source.open('/'+path,predicate=col('bucket')<=threshold-1) as item:
                with closing(item.as_records()) as reader:
                    yield from reader
        else:
            # Parse directly from the data socket: no complete download or payload file.
            with self.ftp.transfercmd('RETR '+path) as sock:
                with sock.makefile('rb') as raw:
                    with csv.open_csv(raw,read_options=csv.ReadOptions(block_size=65536,use_threads=False),
                            convert_options=csv.ConvertOptions(column_types=SCHEMA)) as reader:
                        for batch in reader:
                            yield batch.filter(pc.less_equal(batch.column(1),threshold-1))
            self.ftp.voidresp()

class ClientResources(Resources):
    """Remote mode samples only this client; no remote PID is treated as local."""
    def __init__(self):super().__init__(os.getpid())
    def processes(self):return []

def trial(client,path,threshold,fixture,servers):
    hashes=[hashlib.sha256() for _ in NAMES];count=0;first=None;arrow_bytes=0
    remote=getattr(servers,'remote',False)
    with (ClientResources() if remote else Resources(servers.processes[client.method].pid)) as resources:
        start=time.perf_counter()
        for batch in client.batches(path,threshold):
            if batch.num_rows and first is None:first=time.perf_counter()-start
            count+=batch.num_rows;arrow_bytes+=batch.nbytes;fingerprint(batch,hashes)
        actual=dict(rows=count,sha256=[d.hexdigest() for d in hashes])
        if actual!=fixture['expected'][str(threshold)]:raise AssertionError('result row count or column digest mismatch')
        wall=time.perf_counter()-start
    return dict(state='succeeded',wall_s=wall,first_result_s=first,rows=count,
        sha256=actual['sha256'],result_arrow_bytes=arrow_bytes,
        source_bytes=fixture['bytes'],ftp_download_bytes=fixture['bytes'] if client.method!='dacp-tls' else None,
        client_cpu_s=resources.client_cpu,server_cpu_s=None if remote else resources.server_cpu,
        client_peak_rss_bytes=resources.client_peak,server_peak_rss_bytes=None if remote else resources.peak)

def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output',type=Path,required=True);p.add_argument('--vsftpd',type=Path,default=Path.home()/'.local/share/dacp-transfer-tools/extracted/usr/sbin/vsftpd')
    p.add_argument('--remote-config',type=Path,help='Existing remote servers, trusted public certificate and fixture manifest')
    p.add_argument('--profile',choices=['smoke','final'],default='final')
    p.add_argument('--repeats',type=int,default=3);p.add_argument('--warmups',type=int,default=1)
    a=p.parse_args()
    if a.repeats<1 or a.warmups<0:p.error('repeats must be positive; warmups nonnegative')
    out=a.output.resolve();out.mkdir(parents=True,exist_ok=False);(out/'data').mkdir()
    remote=json.loads(a.remote_config.read_text()) if a.remote_config else None
    if remote:
        # This benchmark measures the direct WAN path, not an environment proxy.
        for key in ['grpc_proxy','GRPC_PROXY','http_proxy','https_proxy','HTTP_PROXY','HTTPS_PROXY','all_proxy','ALL_PROXY']:
            os.environ.pop(key,None)
    fixtures=dict(remote['fixtures']) if remote else {}
    for rows in ([] if remote else [10000] if a.profile=='smoke' else [65536,655360]):
        name=f'observations-{rows}.csv';fixtures[name]=generate(out/'data'/name,rows)
    data=dict(timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),
        config=dict(profile=a.profile,repeats=a.repeats,warmups=a.warmups,seed=20261108),
        host=dict(platform=platform.platform(),logical_cpus=os.cpu_count(),memory_bytes=psutil.virtual_memory().total,network=remote['network'] if remote else 'IPv4 loopback; one shared host'),
        python=sys.version,pyarrow=pa.__version__,runtime_inventory=runtime_inventory(a.vsftpd.resolve()),
        fixtures=fixtures,source_sha256={p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in [Path(__file__),Path(__file__).with_name('suite.py')]},samples=[])
    if remote:data['remote_environment']=remote['environment'];data['remote_runtime_inventory']=remote['runtime_inventory']
    write_json(out/'results.json',data)
    servers=Servers.__new__(Servers);servers.processes={};servers.logs=[]
    try:
        if remote:
            servers=SimpleNamespace(remote=True,out=out,host=remote['host'],ca=Path(remote['ca']).resolve(),ports=remote['ports'])
        else:servers.__init__(out,a.vsftpd.resolve(),publication={'policy':'public'})
        rng=random.Random(20261108)
        for name,fixture in fixtures.items():
            for repeat in range(-a.warmups,a.repeats):
                cases=[(t,m) for t in THRESHOLDS for m in METHODS];rng.shuffle(cases)
                for threshold,method in cases:
                    sample=dict(file=name,threshold=threshold,requested_selectivity=threshold/10000,
                                method=method,repeat=repeat,warmup=repeat<0)
                    client=None
                    try:
                        # Each trial has a fresh client, connected before timing for all methods.
                        client=Client(method,servers)
                        sample.update(trial(client,name,threshold,fixture,servers))
                    except Exception as e:sample.update(state='failed',error=str(e))
                    finally:
                        if client:client.close()
                    data['samples'].append(sample);write_json(out/'results.json',data)
                    print(json.dumps(sample),flush=True)
    finally:
        if not remote:servers.close()
    if any(s['state']!='succeeded' for s in data['samples']):raise SystemExit('Failed trials; see results.json')

if __name__=='__main__':main()

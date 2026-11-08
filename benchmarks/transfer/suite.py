#!/usr/bin/env python3
"""Isolated localhost file-transfer benchmark: DACP/TLS, FTPS and plain FTP."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import random
import re
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import psutil
import pycurl
import yaml

ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'dacp-client'))
from dacp import Source

METHODS=['dacp-tls','ftps','ftp-plain']
CHUNK=65536

def write_json(path,data):
    temporary=path.with_suffix('.tmp');temporary.write_text(json.dumps(data,indent=2,default=str)+'\n');temporary.replace(path)

def runtime_inventory(vsftpd):
    version=subprocess.run([str(vsftpd),'-v'],capture_output=True,text=True)
    binary=vsftpd.read_bytes()
    embedded=re.search(rb'vsftpd: version ([0-9.]+)',binary)
    version_text=(version.stdout+version.stderr).strip() or (embedded.group(1).decode() if embedded else 'unknown')
    return {'vsftpd_version':version_text,
            'vsftpd_binary_sha256':hashlib.sha256(binary).hexdigest(),
            'server_artifact_sha256':{p.name:hashlib.sha256(p.read_bytes()).hexdigest() for p in (ROOT/'dacp-server/target/universal/stage/lib').glob('*dacp-server*.jar')},
            'client_source_sha256':{str(p.relative_to(ROOT)):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((ROOT/'dacp-client/dacp').glob('*.py'))}}

def free_port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]

def generate(directory,profile):
    directory.mkdir()
    sizes={'large-100MiB.bin':100*1048576,'large-1GiB.bin':1024*1048576}
    count=1000;small=65536
    if profile=='smoke':sizes={'large-100MiB.bin':1048576,'large-1GiB.bin':4*1048576};count=10;small=16384
    files={}
    for name,size in list(sizes.items())+[(f'small/{i:04d}.bin',small) for i in range(count)]:
        path=directory/name;path.parent.mkdir(exist_ok=True)
        digest=hashlib.sha256()
        with path.open('wb') as out:
            for offset in range(0,size,1048576):
                block=hashlib.shake_256(f'dacp-transfer-v1:{name}:{offset}'.encode()).digest(min(1048576,size-offset))
                out.write(block);digest.update(block)
        files[name]={'bytes':size,'sha256':digest.hexdigest()}
    return files

def cases(files):
    small=sorted(n for n in files if n.startswith('small/'))
    return [dict(name=name,paths=paths,parallelism=p,connection=c) for name,paths,p,c in [
        ('large-100MiB',['large-100MiB.bin'],1,'reused'),
        ('large-1GiB',['large-1GiB.bin'],1,'reused'),
        ('small-batch',small,1,'reused'),
        ('parallel-4',['large-100MiB.bin']*4,4,'reused'),
        ('parallel-8',['large-100MiB.bin']*8,8,'reused'),
        ('large-100MiB-fresh',['large-100MiB.bin'],1,'fresh'),
        ('small-batch-fresh',small,1,'fresh')]]

class Servers:
    def __init__(self,out,vsftpd):
        self.out=out;self.processes={};self.logs=[]
        self.ports={m:free_port() for m in METHODS}
        tls=out/'tls';tls.mkdir()
        subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','2','-keyout',str(tls/'key.pem'),'-out',str(tls/'cert.pem'),'-subj','/CN=localhost','-addext','subjectAltName=DNS:localhost,IP:127.0.0.1'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
        (tls/'key.pem').chmod(0o600)
        credential=out/'credentials'
        result=subprocess.run([str(ROOT/'dacp-server/target/universal/stage/bin/dacp-admin'),'create-account',str(credential),'--subject','transfer-benchmark','--role','reader'],check=True,capture_output=True,text=True)
        self.token=json.loads(result.stdout)['token']
        config=dict(host='127.0.0.1',public_host='127.0.0.1',port=self.ports['dacp-tls'],tls_cert=str(tls/'cert.pem'),tls_key=str(tls/'key.pem'),tls_ca=str(tls/'cert.pem'),credential_directory=str(credential),datasets={'bench':{'provider':'directory','description':json.dumps({'title':'Transfer benchmark','description':'Deterministic raw binary fixtures'}),'source':{'root':str(out/'data')}}},web={'enabled':False},s3={'enabled':False},remote_operator_library_urls=[],access_log={'directory':str(out/'access-log')},byte_chunk=CHUNK)
        (out/'server.yaml').write_text(yaml.safe_dump(config))
        self.start('dacp-tls',[str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'),str(out/'server.yaml')])
        empty=out/'empty';empty.mkdir();subprocess.run(['sudo','-n','chown','root:root',str(empty)],check=True)
        for method in ['ftps','ftp-plain']:
            options=dict(listen='YES',listen_ipv6='NO',listen_address='127.0.0.1',listen_port=self.ports[method],background='NO',anonymous_enable='YES',no_anon_password='YES',local_enable='NO',write_enable='NO',ftp_username='bin',nopriv_user='nobody',anon_root=str(out/'data'),secure_chroot_dir=str(empty),pasv_enable='YES',port_enable='NO',anon_world_readable_only='YES',xferlog_enable='NO',syslog_enable='NO',max_clients=32,max_per_ip=32,ssl_enable='YES' if method=='ftps' else 'NO')
            if method=='ftps':options.update(allow_anon_ssl='YES',force_anon_logins_ssl='YES',force_anon_data_ssl='YES',require_ssl_reuse='NO',ssl_sslv2='NO',ssl_sslv3='NO',rsa_cert_file=str(tls/'cert.pem'),rsa_private_key_file=str(tls/'key.pem'))
            path=out/(method+'.conf');path.write_text(''.join(f'{k}={v}\n' for k,v in options.items()))
            subprocess.run(['sudo','-n','chown','root:root',str(path)],check=True)
            self.start(method,['sudo','-n',str(vsftpd),str(path)])
        for method,port in self.ports.items():
            for _ in range(200):
                if self.processes[method].poll() is not None:raise RuntimeError(method+' exited; see server log')
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.2):break
                except OSError:time.sleep(.1)
            else:raise TimeoutError(method+' startup timeout')
    def start(self,name,args):
        log=(self.out/(name+'.log')).open('wb');self.logs.append(log)
        self.processes[name]=subprocess.Popen(args,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    def close(self):
        for p in self.processes.values():
            if p.poll() is None:
                # vsftpd drops privileges; sudo sends the signal to the isolated process group.
                subprocess.run(['sudo','-n','kill','-TERM','--','-'+str(p.pid)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
                try:p.wait(timeout=15)
                except subprocess.TimeoutExpired:
                    subprocess.run(['sudo','-n','kill','-KILL','--','-'+str(p.pid)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL);p.wait()
        for log in self.logs:log.close()

class Session:
    def __init__(self,method,servers):
        self.method=method;self.servers=servers
        if method=='dacp-tls':
            self.ds=Source.open(f'dacp://127.0.0.1:{servers.ports[method]}/bench',tls_root_certs=(servers.out/'tls/cert.pem').read_bytes(),token=servers.token)
        else:
            self.curl=pycurl.Curl();self.curl.setopt(pycurl.USERPWD,'anonymous:benchmark@example.invalid');self.curl.setopt(pycurl.PROXY,'')
            self.curl.setopt(pycurl.CONNECTTIMEOUT,30);self.curl.setopt(pycurl.TIMEOUT,300);self.curl.setopt(pycurl.BUFFERSIZE,CHUNK)
            self.curl.setopt(pycurl.SSL_SESSIONID_CACHE,1)
            if method=='ftps':
                self.curl.setopt(pycurl.USE_SSL,pycurl.USESSL_ALL);self.curl.setopt(pycurl.FTPSSLAUTH,pycurl.FTPAUTH_TLS)
                self.curl.setopt(pycurl.CAINFO,str(servers.out/'tls/cert.pem'));self.curl.setopt(pycurl.SSL_VERIFYPEER,1);self.curl.setopt(pycurl.SSL_VERIFYHOST,2)
    def url(self,path):return f'ftp://127.0.0.1:{self.servers.ports[self.method]}/'+path
    def prepare(self,path):
        if self.method=='dacp-tls':
            with self.ds.open('/'+path):pass
        else:
            self.curl.setopt(pycurl.URL,self.url(path));self.curl.setopt(pycurl.WRITEFUNCTION,lambda block:len(block));self.curl.setopt(pycurl.NOBODY,1);self.curl.perform();self.curl.setopt(pycurl.NOBODY,0)
    def transfer(self,path,consume):
        if self.method=='dacp-tls':
            with self.ds.open('/'+path) as item:
                with item.as_bytes() as reader:
                    while True:
                        chunk=reader.read(CHUNK)
                        if not chunk:break
                        consume(chunk)
        else:
            self.curl.setopt(pycurl.URL,self.url(path));self.curl.setopt(pycurl.WRITEFUNCTION,consume);self.curl.perform()
    def close(self):
        if self.method=='dacp-tls':self.ds.close()
        else:self.curl.close()

class Resources:
    """50 ms RSS sampling; CPU includes waited children and living descendants."""
    def __init__(self,pid):self.parent=psutil.Process(pid);self.stop=threading.Event();self.peak=0;self.client_peak=0;self.cpu0=self.cpu();self.thread=threading.Thread(target=self.sample,daemon=True)
    def processes(self):
        try:return [self.parent]+self.parent.children(recursive=True)
        except psutil.Error:return []
    def cpu(self):
        total=0
        for p in self.processes():
            try:
                t=p.cpu_times();total+=t.user+t.system+t.children_user+t.children_system
            except psutil.Error:pass
        return total
    def sample(self):
        while not self.stop.is_set():
            rss=0
            for p in self.processes():
                try:rss+=p.memory_info().rss
                except psutil.Error:pass
            self.peak=max(self.peak,rss);self.client_peak=max(self.client_peak,psutil.Process().memory_info().rss)
            self.stop.wait(.05)
    def __enter__(self):self.thread.start();self.client0=time.process_time();return self
    def __exit__(self,*args):self.client_cpu=time.process_time()-self.client0;self.server_cpu=max(0,self.cpu()-self.cpu0);self.stop.set();self.thread.join()

def run_case(case,method,sink,servers,files,trial):
    paths=case['paths'];parallel=case['parallelism'];partitions=[list(enumerate(paths))[i::parallel] for i in range(parallel)]
    sessions=[]
    if case['connection']=='reused':
        try:
            for part in partitions:
                session=Session(method,servers);sessions.append(session);session.prepare(part[0][1])
        except BaseException:
            for session in sessions:session.close()
            raise
    trial.mkdir();first=[];lock=threading.Lock()
    def worker(index):
        session=sessions[index] if sessions else Session(method,servers)
        try:
            for ordinal,name in partitions[index]:
                digest=hashlib.sha256();size=0;output=(trial/f'{ordinal}.bin').open('wb') if sink=='disk-fsync' else None
                def consume(block):
                    nonlocal size
                    if not size:
                        with lock:first.append(time.perf_counter())
                    size+=len(block);digest.update(block)
                    if output:output.write(block)
                    return len(block)
                try:
                    session.transfer(name,consume)
                    if output:output.flush();os.fsync(output.fileno())
                finally:
                    if output:output.close()
                if size!=files[name]['bytes'] or digest.hexdigest()!=files[name]['sha256']:raise ValueError('received payload failed independent size/SHA-256 check')
        finally:
            if not sessions:session.close()
    start=time.perf_counter()
    try:
        with Resources(servers.processes[method].pid) as resources:
            with ThreadPoolExecutor(max_workers=parallel) as pool:list(pool.map(worker,range(parallel)))
        wall=time.perf_counter()-start
        size=sum(files[name]['bytes'] for name in paths)
        return dict(wall_s=wall,first_byte_s=min(first)-start,payload_bytes=size,MiB_s=size/1048576/wall,client_cpu_s=resources.client_cpu,server_cpu_s_approx=resources.server_cpu,server_peak_rss_bytes=resources.peak,client_peak_rss_bytes=resources.client_peak,state='succeeded',verified_files=len(paths))
    finally:
        for session in sessions:session.close()
        for path in trial.glob('*.bin'):path.unlink()
        trial.rmdir()

def main():
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--output',type=Path,required=True);parser.add_argument('--vsftpd',type=Path,required=True);parser.add_argument('--profile',choices=['smoke','final'],default='final');parser.add_argument('--repeats',type=int,default=3);parser.add_argument('--warmups',type=int,default=1);parser.add_argument('--seed',type=int,default=20261105)
    args=parser.parse_args();assert args.repeats>0 and args.warmups>=0
    out=args.output.resolve();out.mkdir(parents=True,exist_ok=False);out.chmod(0o755)
    data=dict(timestamp=time.strftime('%Y-%m-%dT%H:%M:%SZ',time.gmtime()),config={'profile':args.profile,'repeats':args.repeats,'warmups':args.warmups,'seed':args.seed},host={'platform':platform.platform(),'logical_cpus':os.cpu_count(),'memory_bytes':psutil.virtual_memory().total,'network':'IPv4 loopback; one shared host; no physical network'},versions={n:importlib.metadata.version(n) for n in ['pyarrow','psutil','pycurl','protobuf','PyYAML']},curl=pycurl.version,python=sys.version,openssl=ssl.OPENSSL_VERSION,git_commit=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip(),git_dirty=bool(subprocess.check_output(['git','status','--porcelain'],cwd=ROOT)),samples=[])
    data['runtime_inventory']=runtime_inventory(args.vsftpd.resolve())
    data['files']=generate(out/'data',args.profile);data['cases']=cases(data['files']);write_json(out/'results.json',data)
    servers=None
    try:
        servers=Servers.__new__(Servers);servers.processes={};servers.logs=[];servers.__init__(out,args.vsftpd.resolve())
        rng=random.Random(args.seed)
        for case in data['cases']:
            for sink in ['consume','disk-fsync']:
                for repeat in range(-args.warmups,args.repeats):
                    methods=METHODS.copy();rng.shuffle(methods)
                    for method in methods:
                        sample={'case':case['name'],'method':method,'sink':sink,'connection':case['connection'],'parallelism':case['parallelism'],'repeat':repeat,'warmup':repeat<0}
                        started=time.perf_counter()
                        try:sample.update(run_case(case,method,sink,servers,data['files'],out/'receiving'))
                        except Exception as error:sample.update(state='failed',error=str(error),wall_s=time.perf_counter()-started)
                        data['samples'].append(sample);write_json(out/'results.json',data);print(json.dumps(sample),flush=True)
    finally:
        if servers:servers.close()
    if any(s['state']!='succeeded' for s in data['samples']):raise SystemExit('Failed transfer trials; retained in results.json')

if __name__=='__main__':main()

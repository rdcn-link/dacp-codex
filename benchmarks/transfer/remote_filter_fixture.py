#!/usr/bin/env python3
"""Isolated public-WAN CSV filtering servers; no existing services are changed."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import yaml

p=argparse.ArgumentParser(description=__doc__)
p.add_argument('action',choices=['start','stop'])
p.add_argument('--root',type=Path,required=True)
p.add_argument('--public-ip',required=True);p.add_argument('--client-ip',required=True)
a=p.parse_args();root=a.root.resolve()
assert str(root).startswith('/var/snap/docker/common/dacp-benchmarks/'),'Dedicated benchmark root required'
root.mkdir(parents=True,exist_ok=True)
registry=root/'processes.json';ports={'dacp-tls':28801,'ftps':28802,'ftp-plain':28803}
shared=['-p','tcp','-m','multiport','--dports','28801:28803,28810:28829']
comment=['-m','comment','--comment','dacp-csv-filter-benchmark']
rules=[['-i','lo',*shared,*comment,'-j','ACCEPT'],[*shared,'-s',a.client_ip,*comment,'-j','ACCEPT'],[*shared,*comment,'-j','DROP']]
if a.action=='stop':
    records=json.loads(registry.read_text()) if registry.exists() else {}
    for r in records.values():
        stat=Path(f"/proc/{r['pid']}/stat")
        if stat.exists() and stat.read_text().split()[21]==r['start']:os.killpg(r['pid'],signal.SIGTERM)
    deadline=time.monotonic()+30
    for r in records.values():
        stat=Path(f"/proc/{r['pid']}/stat")
        while stat.exists() and stat.read_text().split()[2]!='Z' and stat.read_text().split()[21]==r['start']:
            if time.monotonic()>deadline:raise RuntimeError('Fixture process still stopping; registry retained')
            time.sleep(.1)
    for rule in rules:subprocess.run(['iptables','-D','INPUT',*rule],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    registry.unlink(missing_ok=True);print('Only registered CSV benchmark services/rules stopped');raise SystemExit
assert not registry.exists(),'Stop the existing fixture first'
for port in [*ports.values(),*range(28810,28830)]:
    with socket.socket() as sock:
        sock.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);sock.bind(('0.0.0.0',port))
manifest=json.loads((root/'fixtures.json').read_text())
for name,f in manifest.items():
    path=root/'data'/name
    assert path.stat().st_size==f['bytes'] and hashlib.sha256(path.read_bytes()).hexdigest()==f['sha256'],'Fixture mismatch'
tls=root/'tls';tls.mkdir(exist_ok=True)
if not (tls/'cert.pem').exists():
    subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-days','7','-keyout',str(tls/'key.pem'),'-out',str(tls/'cert.pem'),'-subj','/CN=DACP CSV benchmark','-addext',f'subjectAltName=DNS:localhost,IP:127.0.0.1,IP:{a.public_ip}'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
(tls/'key.pem').chmod(0o600)
config=dict(host='0.0.0.0',public_host=a.public_ip,port=ports['dacp-tls'],tls_cert=str(tls/'cert.pem'),tls_key=str(tls/'key.pem'),tls_ca=str(tls/'cert.pem'),credential_directory=str(root/'credentials'),datasets={'bench':dict(provider='directory',description=json.dumps(dict(title='CSV filtering benchmark',description='Deterministic public synthetic CSV fixtures',publication=dict(policy='public'))),source=dict(root=str(root/'data')))},web=dict(enabled=False),s3=dict(enabled=False),remote_operator_library_urls=[],access_log=dict(directory=str(root/'access-log')),byte_chunk=65536)
(root/'server.yaml').write_text(yaml.safe_dump(config))
empty=root/'empty';empty.mkdir(exist_ok=True)
for index,method in enumerate(['ftps','ftp-plain']):
    options=dict(listen='YES',listen_ipv6='NO',listen_address='0.0.0.0',listen_port=ports[method],background='NO',anonymous_enable='YES',no_anon_password='YES',local_enable='NO',write_enable='NO',ftp_username='bin',nopriv_user='nobody',anon_root=str(root/'data'),secure_chroot_dir=str(empty),pasv_enable='YES',port_enable='NO',pasv_address=a.public_ip,pasv_min_port=28810+index*10,pasv_max_port=28819+index*10,anon_world_readable_only='YES',xferlog_enable='NO',syslog_enable='NO',max_clients=32,max_per_ip=32,ssl_enable='YES' if method=='ftps' else 'NO')
    if method=='ftps':options.update(allow_anon_ssl='YES',force_anon_logins_ssl='YES',force_anon_data_ssl='YES',require_ssl_reuse='NO',ssl_sslv2='NO',ssl_sslv3='NO',rsa_cert_file=str(tls/'cert.pem'),rsa_private_key_file=str(tls/'key.pem'))
    (root/(method+'.conf')).write_text(''.join(f'{k}={v}\n' for k,v in options.items()))
for rule in reversed(rules):subprocess.check_call(['iptables','-I','INPUT','1',*rule])
processes={}
for method in ports:
    args=[root/'node/bin/dacp-server',root/'server.yaml'] if method=='dacp-tls' else [root/'vsftpd',root/(method+'.conf')]
    with (root/(method+'.log')).open('ab') as log:
        proc=subprocess.Popen([str(x) for x in args],cwd=root,stdin=subprocess.DEVNULL,stdout=log,stderr=subprocess.STDOUT,start_new_session=True,env={**os.environ,'JAVA_OPTS':'-Xms64m -Xmx512m'})
    processes[method]=dict(pid=proc.pid,start=Path(f'/proc/{proc.pid}/stat').read_text().split()[21]);registry.write_text(json.dumps(processes))
    for _ in range(180):
        if proc.poll() is not None:raise RuntimeError(method+' failed; see fixture log')
        try:
            with socket.create_connection(('127.0.0.1',ports[method]),timeout=.2):break
        except OSError:time.sleep(.5)
    else:raise RuntimeError(method+' did not become ready')
print('Remote CSV fixture ready; DACP 28801, FTPS 28802, FTP 28803; client IP restricted')

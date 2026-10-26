#!/usr/bin/env python3
"""Real Flight/Docker metadata workers: reuse, fresh computations, isolation, recovery, and cleanup."""
import argparse
import json
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import zipfile
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'dacp-client'))
from dacp.inference import infer
from security_test_support import authorize
PYTHON = '''import os,time,uuid
from dacp_operator import descriptor,result,run_operator
instance=uuid.uuid4().hex
calls=0
def process(ctx):
    raise AssertionError('inference must not execute business processing')
def infer_outputs(inputs,params):
    global calls
    calls+=1
    if params.get('fail'):raise ValueError('requested probe failure')
    if params.get('sleep'):time.sleep(params['sleep'])
    return result({'result':descriptor(['bytes'],[params['format']],certainty='known',structure={'instance':instance,'pid':os.getpid(),'calls':calls})})
if __name__=='__main__':run_operator(process,infer_outputs)
'''
JAVA = '''import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
public class InferenceProbe {
  static final String instance=java.util.UUID.randomUUID().toString();
  static int calls=0;
  public static void main(String[] args) {
    dacp.operator.OperatorRunner.infer(args,System.out,new scala.runtime.AbstractFunction2<scala.collection.immutable.Map<String,dacp.StreamDescriptor>,JsonNode,dacp.InferenceResult>() {
      public dacp.InferenceResult apply(scala.collection.immutable.Map<String,dacp.StreamDescriptor> inputs,JsonNode params) {
        calls++;
        if(params.path("fail").asBoolean())throw new IllegalArgumentException("requested probe failure");
        ObjectNode n=dacp.StructureInference.mapper().createObjectNode();n.putArray("diagnostics");
        ObjectNode d=n.putObject("outputs").putObject("result");
        d.putArray("representations").add("bytes");d.putArray("formats").add(params.path("format").asText());d.put("certainty","known");
        d.putObject("structure").put("instance",instance).put("calls",calls).put("pid",ProcessHandle.current().pid());
        return dacp.StructureInference.result(n);
      }
    });
  }
}
'''
def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--docker-prefix',default='')
    parser.add_argument('--lifecycle-only',action='store_true',help='Run caller isolation, quiet requests, and shutdown checks only')
    args=parser.parse_args();prefix=shlex.split(args.docker_prefix)
    def containers():
        r=subprocess.run(prefix+['docker','ps','-aq','--filter','label=dacp.purpose=structure_inference'],capture_output=True,text=True,check=True)
        return set(r.stdout.split())
    original_containers=containers()
    with tempfile.TemporaryDirectory(prefix='dacp-inference-live-') as tmp:
        d=Path(tmp);cert=ROOT/'examples/certs';(d/'probe.py').write_text(PYTHON)
        (d/'InferenceProbe.java').write_text(JAVA)
        cp=str(ROOT/'dacp-server/target/universal/stage/lib/*')
        subprocess.run(['javac','-cp',cp,str(d/'InferenceProbe.java')],check=True)
        jar=d/'probe.jar'
        subprocess.run(['jar','cf',str(jar),'-C',str(d),'InferenceProbe.class','-C',str(d),'InferenceProbe$1.class'],check=True)
        wheel=d/'worker_probe-1.0-py3-none-any.whl'
        with zipfile.ZipFile(wheel,'w') as z:
            z.writestr('probe.py',PYTHON)
            z.writestr('worker_probe-1.0.dist-info/METADATA','Metadata-Version: 2.1\nName: worker-probe\nVersion: 1.0\n')
            z.writestr('worker_probe-1.0.dist-info/WHEEL','Wheel-Version: 1.0\nGenerator: dacp-test\nRoot-Is-Purelib: true\nTag: py3-none-any\n')
            z.writestr('worker_probe-1.0.dist-info/RECORD','')
        def publish(name,executor,image,entry,artifact=None,**extra):
            folder=d/'operators'/name/'1';folder.mkdir(parents=True)
            manifest=dict(name=name,version='1',image=image,executor=executor,entrypoint=entry,inputs={},**extra)
            if artifact:manifest['artifact']=str(artifact)
            (folder/'operator.yaml').write_text(json.dumps(manifest))
        publish('python','python','dacp/operator-python:1.0','probe',d/'probe.py')
        publish('wheel','python','dacp/operator-python:1.0','probe',wheel)
        publish('java','jvm','dacp/operator-jvm:1.1','InferenceProbe',jar)
        publish('native','command','dacp/operator-python:1.0','/usr/bin/does-not-exist',executor_config={'args':[],'stdin':None,'stdout':{'port':'result','representation':'bytes','format':'text'}})
        with socket.socket() as sock:sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
        origin=f'dacp://localhost:{port}'
        config=dict(host='127.0.0.1',public_host='localhost',port=port,datasets={},tls_cert=str(cert/'node-a.crt'),tls_key=str(cert/'node-a.key'),tls_ca=str(cert/'ca.crt'),local_operator_library=str(d/'operators'),operator_cache=str(d/'cache'),access_log={'directory':str(d/'access')},web={'enabled':False},s3={'enabled':False},inference_workers={'max_workers':2,'idle_seconds':8 if args.lifecycle_only else 3,'timeout_seconds':90})
        token=authorize(config,d);other_token=authorize(config,d,subject='another-live-test');(d/'node.yaml').write_text(json.dumps(config))
        with (d/'server.log').open('w') as log:
            process=subprocess.Popen(prefix+[str(ROOT/'dacp-server/target/universal/stage/bin/dacp-server'),str(d/'node.yaml')],cwd=d,stdout=log,stderr=subprocess.STDOUT)
        def call(name,params=None,token_override=None,**options):
            return infer(origin,{'operator':name+'@1','inputs':{},'params':params or {'format':'text'}},token=token_override or token,tls_root_certs=(cert/'ca.crt').read_bytes(),**options)['outputs']['result']
        try:
            for _ in range(200):
                if process.poll() is not None:raise RuntimeError((d/'server.log').read_text())
                try:
                    with socket.create_connection(('127.0.0.1',port),timeout=.1):break
                except OSError:time.sleep(.1)
            else:raise TimeoutError('test server did not start')
            for name in (() if args.lifecycle_only else ('python','java','wheel')):
                start=time.monotonic();first=call(name,{'format':'json'});cold=time.monotonic()-start
                start=time.monotonic();second=call(name,{'format':'text'});warm=time.monotonic()-start
                assert first['formats']==['json'] and second['formats']==['text']
                assert first['structure']['instance']==second['structure']['instance'] and second['structure']['calls']==2
                print(name,'reuse OK; cold',round(cold,3),'s; warm',round(warm,3),'s',flush=True)
                try:call(name,{'fail':True});raise AssertionError('expected method failure')
                except Exception as error:
                    if isinstance(error,AssertionError):raise
                rebuilt=call(name);assert rebuilt['structure']['instance']!=first['structure']['instance'] and rebuilt['structure']['calls']==1
                time.sleep(4.2)
                after_idle=call(name);assert after_idle['structure']['instance']!=rebuilt['structure']['instance']
                print(name,'failure recovery and idle reclamation OK',flush=True)
            if args.lifecycle_only:
                first=call('python')
                # This exceeds the Docker HTTP response-header timeout, but the live stream must survive.
                quiet=call('python',{'format':'json','sleep':6})
                assert quiet['structure']['instance']==first['structure']['instance']
                other=call('python',token_override=other_token)
                assert other['structure']['instance']!=first['structure']['instance']
                (d/'probe.py').write_text(PYTHON+'\n# new artifact revision\n')
                revised=call('python')
                assert revised['structure']['instance']!=first['structure']['instance']
                print('quiet requests, caller isolation, and artifact revision isolation OK',flush=True)
            # The native adapter must infer without launching the nonexistent executable.
            assert call('native')['formats']==['text']
            first=call('python')
            try:call('python',{'format':'text','sleep':30},timeout=.5);raise AssertionError('expected cancellation')
            except Exception as error:
                if isinstance(error,AssertionError):raise
            time.sleep(1)
            rebuilt=call('python');assert rebuilt['structure']['instance']!=first['structure']['instance']
            print('native metadata-only adapter and cancelled-request recovery OK',flush=True)
        except BaseException:
            print((d/'server.log').read_text()[-10000:],file=sys.stderr);raise
        finally:
            process.terminate()
            try:process.wait(timeout=20)
            except subprocess.TimeoutExpired:process.kill();process.wait()
        assert not (containers()-original_containers), 'inference containers leaked after server shutdown'
        print('Worker integration checks passed; no worker containers remain after shutdown',flush=True)
if __name__=='__main__':main()

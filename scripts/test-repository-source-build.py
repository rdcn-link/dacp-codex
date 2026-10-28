#!/usr/bin/env python3
"""Real Docker source-build smoke test; uses a temporary repository and never publishes into the live catalog."""
import argparse
import hashlib
import io
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
from urllib.request import Request, urlopen
from urllib.error import HTTPError
import zipfile

ROOT=Path(__file__).resolve().parents[1]


def main():
    p=argparse.ArgumentParser(description=__doc__)
    p.add_argument('--runtime-manifest',type=Path,required=True)
    p.add_argument('--catalog',type=Path,required=True)
    p.add_argument('--docker-group',action='store_true')
    p.add_argument('--chromium',help='Also exercise administrator approval through the browser')
    args=p.parse_args()
    runtime=json.loads(args.runtime_manifest.read_text())['runtime']
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));port=sock.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix='.dacp-real-source-build-',dir=args.catalog.resolve().parent) as temporary:
        work=Path(temporary);catalog=work/'repository';catalog.mkdir();blobs=catalog/'blobs/sha256';blobs.mkdir(parents=True)
        archive=runtime['archive'];os.link(args.catalog/'blobs/sha256'/archive['sha256'],blobs/archive['sha256'])
        launcher=ROOT/'dacp-operator-repository/target/universal/stage/bin/dacp-operator-repository'
        command=(["sudo","-n","-u","cnic","-g","docker"] if args.docker_group else [])+[str(launcher),'serve','--root',str(catalog),'--port',str(port)]
        origin=f'http://localhost:{port}';cookie='';csrf=''
        def request(path,method='GET',n=None,token=None,expected=200,raw=None):
            data=raw if raw is not None else None if n is None else json.dumps(n).encode()
            headers={'Content-Type':'application/octet-stream' if raw is not None else 'application/json'}
            if token:headers['Authorization']='Bearer '+token
            elif cookie:headers.update(Cookie=cookie,**{'X-CSRF-Token':csrf})
            try:
                with urlopen(Request(origin+path,data,headers,method=method),timeout=30) as r:
                    assert r.status==expected,(path,r.status)
                    return r.read(),dict(r.headers)
            except HTTPError as e:
                if e.code!=expected:raise RuntimeError(f'{path}: HTTP {e.code}: {e.read().decode()}') from None
                return e.read(),dict(e.headers)
        log=(work/'server.log').open('w');process=subprocess.Popen(command,stdout=log,stderr=log)
        try:
            for _ in range(100):
                if process.poll() is not None:raise RuntimeError('Repository startup failed')
                try:request('/health');break
                except OSError:time.sleep(.1)
            else:raise TimeoutError('Repository startup timeout')
            password=(catalog/'.admin/bootstrap-password').read_text().strip()
            data,headers=request('/api/admin/login','POST',{'password':password});cookie=headers['Set-cookie' if 'Set-cookie' in headers else 'Set-Cookie'].split(';')[0];csrf=json.loads(data)['csrf']
            token=json.loads(request('/api/admin/tokens','POST',{'submitter':'smoke-test'},expected=201)[0])['token']
            rt={'name':'python-arrow','version':'smoke','description':'Shared Python build/execution runtime','image':runtime['image'],'archive':archive,'executors':['python'],'dependencies':{'python':'3.12','pyarrow':'23.0.1'},'build':{'image':runtime['image'],'archive':archive,'languages':['python']}}
            request('/api/admin/runtimes','POST',rt,expected=201)
            source=io.BytesIO()
            with zipfile.ZipFile(source,'w',zipfile.ZIP_DEFLATED) as z:
                z.writestr('src/example.py',"from dacp_operator import BytesOperator\nclass Example(BytesOperator):\n def transform(self,chunk,params): return chunk\nif __name__ == '__main__': Example().main()\n")
                for name in ('__init__.py','inference.py'):
                    z.writestr('src/dacp_operator/'+name,(ROOT/'dacp-operator-sdk/python/dacp_operator'/name).read_bytes())
            value=source.getvalue();sourceHash=hashlib.sha256(value).hexdigest()
            request('/v1/blobs/sha256/'+sourceHash,'PUT',token=token,raw=value,expected=201)
            manifest={'api_version':2,'name':'source_smoke','version':'1','description':'Real Docker source build','icon':{'path':'M3 5h18v14H3z','color':'#14866b'},'inputs':{'source':{'representations':['bytes'],'formats':[]}},'parameters':{'type':'object','properties':{},'required':[],'additionalProperties':False},'execution':{'executor':'python','entrypoint':'example','config':{}},'runtime':runtime,'build':{'language':'python'},'source_public':False,'provenance':{'source_archive':{'sha256':sourceHash,'size':len(value),'filename':'sources.zip'}}}
            id=json.loads(request('/v1/submissions','POST',manifest,token,202)[0])['submission_id']
            deadline=time.monotonic()+180
            while True:
                s=json.loads(request('/api/admin/submissions/'+id)[0]);status=s['build_result']['status']
                if status in ('succeeded','failed'):break
                if time.monotonic()>deadline:raise TimeoutError('Build smoke test timed out')
                time.sleep(1)
            assert status=='succeeded',s['build_result']
            request('/v1/operators/source_smoke/1',expected=404)
            request('/v1/blobs/sha256/'+sourceHash,expected=404)
            if args.chromium:
                from playwright.sync_api import sync_playwright, expect
                with sync_playwright() as playwright:
                    browser=playwright.chromium.launch(executable_path=args.chromium,args=['--no-sandbox','--disable-dev-shm-usage'])
                    page=browser.new_page();errors=[];page.on('pageerror',lambda e:errors.append(str(e)))
                    page.goto(origin+'/admin')
                    page.locator('#password').fill(password)
                    page.locator('#login-form button').click(force=True)
                    expect(page.locator('#console')).to_be_visible()
                    page.locator('#submission-list button').filter(has_text='source_smoke@1').click(force=True)
                    expect(page.locator('#submission-detail h2')).to_have_text('source_smoke@1')
                    page.locator('#submission-detail input[list]').fill('Verification, Streams')
                    page.locator('#submission-detail input[type=number]').fill('4')
                    page.locator('#submission-detail textarea').fill('Reviewed bundled SDK rule findings and the generated artifact.')
                    page.locator('#submission-detail button[type=submit]').click(force=True)
                    expect(page.locator('#message')).to_contain_text('approved')
                    page.locator('[data-tab=tokens]').click(force=True)
                    page.locator('#submitter').fill('browser-submitter')
                    page.locator('#token-form button').click(force=True)
                    expect(page.locator('#new-token')).to_be_visible()
                    assert len(page.locator('#issued-token').input_value())==64
                    page.locator('#logout').click(force=True)
                    expect(page.locator('#login')).to_be_visible()
                    assert not errors,errors
                    browser.close()
            else:
                request('/api/admin/submissions/'+id+'/review','POST',{'decision':'approved','categories':['Verification','Streams'],'trust_score':4,'note':'Reviewed bundled SDK rule findings and the generated artifact.'})
            published=json.loads(request('/v1/operators/source_smoke/1')[0]);assert published['trust']['score']==4 and len(published['categories'])==2
            assert 'source_archive' not in published['provenance']
            request('/v1/blobs/sha256/'+sourceHash,expected=404)
            artifact=published['artifact'];data=request('/v1/blobs/sha256/'+artifact['sha256']+'/operator.zip')[0]
            assert hashlib.sha256(data).hexdigest()==artifact['sha256']
            with zipfile.ZipFile(io.BytesIO(data)) as z:assert 'example.pyc' in z.namelist() and not any(n.endswith('.py') for n in z.namelist())
            output=work/'operator.zip';output.write_bytes(data)
            inference=work/'request.json';inference.write_text(json.dumps({'inputs':{'source':{'representations':['bytes'],'formats':['binary'],'certainty':'known'}},'params':{}}))
            prefix=['sudo','-n','-u','cnic','-g','docker'] if args.docker_group else []
            result=subprocess.run(prefix+['docker','run','--rm','--network','none','-v',str(work)+':/check:ro','--entrypoint','/usr/local/bin/python',runtime['image'],'-c',"import sys,runpy;sys.path.insert(0,'/check/operator.zip');sys.argv=['example','infer','/check/request.json'];runpy.run_module('example',run_name='__main__')"],check=True,capture_output=True,timeout=60)
            assert json.loads(result.stdout)['outputs']['result']['representations']==['bytes']
            print('PASS: real isolated Python build, source privacy, approval, public trust/categories, downloaded artifact inference')
        finally:
            process.terminate()
            try:process.wait(timeout=20)
            except subprocess.TimeoutExpired:process.kill();process.wait()
            log.close()


if __name__=='__main__':main()

#!/usr/bin/env python3
"""Create chunk-split IPC fixtures and verify all Scala operators in the shared JVM runtime."""
import argparse
import base64
import json
from pathlib import Path
import subprocess
import shlex
import tempfile
import pyarrow as pa
ROOT=Path(__file__).resolve().parents[1]
def main():
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--docker-prefix',default='');a=p.parse_args()
    home=ROOT/'examples/operators/common-scala';specs=json.loads((home/'src/main/resources/catalog.json').read_text())
    jar=home/'target/scala-2.13/dacp-common-scala_2.13-1.0.jar'
    with tempfile.TemporaryDirectory(prefix='dacp-scala-contract-',dir='/tmp') as tmp:
        tmp=Path(tmp);tmp.chmod(0o755)
        for name,s in specs.items():
            d=tmp/name;d.mkdir();data=base64.b64decode(s['sample']);table=pa.table({'data':pa.array([data[i:i+2] for i in range(0,len(data),2)],type=pa.binary())})
            with (d/'input.arrow').open('wb') as f:
                with pa.ipc.new_stream(f,table.schema) as w:w.write_table(table,max_chunksize=2)
            (d/'params.json').write_text(json.dumps(s['params']))
            (d/'manifest.json').write_text(json.dumps([dict(port='source',file='input.arrow',representation='bytes',format='binary')]))
        subprocess.run(shlex.split(a.docker_prefix)+['docker','run','--rm','-v',str(jar)+':/operator/common.jar:ro','-v',str(tmp)+':/fixtures:ro','--entrypoint','/opt/java/bin/java','dacp/operator-jvm:1.1','--add-opens=java.base/java.nio=ALL-UNNAMED','-Dio.netty.tryReflectionSetAccessible=true','-cp','/operator/common.jar:/opt/lib/*','dacp.common.VerifyStreams','/fixtures'],check=True)
if __name__=='__main__':main()

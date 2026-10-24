package dacp.operators

import dacp.{OperatorExecutor,OperatorProgram,OperatorCommand}
import scala.jdk.CollectionConverters._

/** Bridges ordinary stdin/stdout programs to bytes ports, exclusively inside a container. */
final class CommandExecutor extends OperatorExecutor {
  val id = "command"
  def command(p:OperatorProgram, artifactPath:Option[String]):OperatorCommand = {
    val c=p.configuration
    require(c.keySet == Set("args","stdin","stdout"),"command requires args, stdin, stdout settings")
    val args=c("args") match {
      case a:java.util.List[_] => a.asScala.map { case s:String if !s.contains('\u0000') => s
        case _ => throw new IllegalArgumentException("command args must be strings without NUL") }.toVector
      case _ => throw new IllegalArgumentException("command args must be a list")
    }
    val input=c("stdin") match {case null => null;case s:String => s;case _ => throw new IllegalArgumentException("stdin must name a port or be null")}
    val output=c("stdout") match {
      case null=>Map.empty[String,Object]
      case m:java.util.Map[_,_] => m.asScala.iterator.map {
        case (k:String,v) => k -> v
        case _ => throw new IllegalArgumentException("stdout keys must be strings")
      }.toMap
      case _ => throw new IllegalArgumentException("stdout must be a mapping")
    }
    require(c("stdout")==null || output.keySet == Set("port","representation","format"),"stdout requires port, representation, format")
    require(output.isEmpty || output("representation")=="bytes","command stdout supports bytes only")
    val port=output.getOrElse("port",null);val format=output.getOrElse("format",null)
    require(output.isEmpty || (port.isInstanceOf[String] && format.isInstanceOf[String] && format.toString.nonEmpty),"invalid stdout metadata")
    require(p.inputs == (if(input==null)Map.empty[String,Set[String]]else Map(input -> Set("bytes"))),"command inputs must match stdin (a bytes port or null)")
    require(p.outputs == (if(port==null)Map.empty[String,Set[String]]else Map(port.toString -> Set("bytes"))),"command outputs must match stdout (a bytes port mapping or null)")
    require(p.artifact.isDefined==artifactPath.isDefined,"missing executable artifact path")
    require(p.entrypoint.startsWith("/") && !p.entrypoint.contains('\u0000'),"command entrypoint must be an absolute executable path")
    require(p.artifact.isEmpty || p.entrypoint==artifactPath.get,"artifact entrypoint must equal its /operator/<filename> path")
    val config=new com.fasterxml.jackson.databind.ObjectMapper().writeValueAsString(Map(
      "argv" -> (Vector(p.entrypoint)++args).asJava,"stdin" -> input,"port" -> port,"format" -> format,
      "artifact" -> java.lang.Boolean.valueOf(artifactPath.isDefined)).asJava)
    OperatorCommand(Some(Vector("/usr/local/bin/python","-u","-c",CommandBridge.launcher,config)),
      arguments=Vector.empty,environment=Map("PYTHONDONTWRITEBYTECODE"->"1"),executableScratch=artifactPath.isDefined)
  }
}

private[operators] object CommandBridge {
  val launcher = """import json,os,shutil,struct,subprocess,sys,threading
import pyarrow.ipc as ipc

config=json.loads(sys.argv[1])
with open('/input/manifest.json') as f:
    manifest=json.load(f)
if (config['stdin'] is None and manifest) or (config['stdin'] is not None and (len(manifest)!=1 or manifest[0]['port']!=config['stdin'] or manifest[0]['representation']!='bytes')):
    raise ValueError('command requires one bytes input')
argv=config['argv']
if config['artifact']:
    target='/operator-work/program'
    shutil.copyfile(argv[0],target)
    os.chmod(target,0o500)
    argv[0]=target

def frame(kind,data=b'',**metadata):
    header=json.dumps(dict(type=kind,port=config['port'],**metadata)).encode()
    if len(header)>65536:
        raise ValueError('output header too large')
    sys.stdout.buffer.write(struct.pack('>II',len(header),len(data))+header+data)
    sys.stdout.buffer.flush()

process=subprocess.Popen(argv,stdin=subprocess.PIPE if config['stdin'] is not None else subprocess.DEVNULL,stdout=subprocess.PIPE,stderr=None,bufsize=0)
errors=[]
def feed():
    if config['stdin'] is None:
        return
    try:
        with open('/input/'+manifest[0]['file'],'rb') as source:
            with ipc.open_stream(source) as reader:
                for batch in reader:
                    for cell in batch.column(0):
                        data=memoryview(cell.as_py())
                        while data:
                            count=process.stdin.write(data)
                            if not count:
                                raise BrokenPipeError('command closed stdin')
                            data=data[count:]
    except BaseException as error:
        errors.append(error)
        process.kill()
    finally:
        process.stdin.close()

pump=threading.Thread(target=feed,daemon=True)
try:
    pump.start()
    if config['port'] is not None:
        frame('start',representation='bytes',format=config['format'])
    while True:
        chunk=process.stdout.read(65536)
        if not chunk:
            break
        if config['port'] is not None:
            frame('data',chunk)
    status=process.wait()
    if errors:
        raise errors[0]
    if status!=0:
        raise RuntimeError('command exited with status '+str(status))
    pump.join()
    if errors:
        raise errors[0]
    if config['port'] is not None:
        frame('end')
finally:
    if process.poll() is None:
        process.kill()
    process.wait()
    process.stdout.close()
"""
}

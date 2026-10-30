package dacp.operators


import dacp.{OperatorExecutor,OperatorProgram,OperatorCommand}


final class ContainerExecutor extends OperatorExecutor {
  val id = "container"
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(p.configuration.isEmpty,"executor does not accept configuration")
    require(p.artifact.isEmpty && p.entrypoint.isEmpty, "container executor uses its image entrypoint; no code artifact")
    OperatorCommand()
  }
}
final class JvmExecutor extends OperatorExecutor {
  override def inferenceWorkerCommand(p:OperatorProgram,path:Option[String]):OperatorCommand=InferenceLaunch.jvm(command(p,path))
  override def sourceInferenceWorkerCommand(p:OperatorProgram,path:String):OperatorCommand=InferenceLaunch.jvm(sourceCommand(p,path))
  override def groupCommand(group:dacp.OperatorGroup):Option[OperatorCommand]=JvmGroup.command(group)
  val id = "jvm"
  override def sourceCommand(p: OperatorProgram, path: String): OperatorCommand = {
    require(p.entrypoint.matches("[A-Za-z_$][A-Za-z0-9_.$]*"),"JVM entrypoint must be a main class")
    // Compiler and application run inside the same isolated container; no host compilation.
    val script="""val target=java.nio.file.Files.createDirectories(java.nio.file.Path.of("/tmp/classes"))
val source=java.nio.file.Path.of("/tmp/Operator.scala")
java.nio.file.Files.copy(java.nio.file.Path.of(args(0)),source)
val ok=Console.withOut(System.err) { scala.tools.nsc.Main.process(Array("-usejavacp","-d",target.toString,source.toString)) }
require(ok,"Scala compilation failed")
val loader=new java.net.URLClassLoader(Array(target.toUri.toURL),Thread.currentThread.getContextClassLoader)
try { loader.loadClass(args(1)).getMethod("main",classOf[Array[String]]).invoke(null,args.drop(2).asInstanceOf[Object]) }
finally loader.close()
"""
    OperatorCommand(Some(Vector("/opt/java/bin/java","--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true","-cp","/opt/lib/*","scala.tools.nsc.MainGenericRunner",
      "-usejavacp","-e",script,path,p.entrypoint)),arguments=Vector("operator","/input","/input/params.json"))
  }
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(p.configuration.isEmpty,"executor does not accept configuration")
    require(p.entrypoint.matches("[A-Za-z_$][A-Za-z0-9_.$]*"), "JVM entrypoint must be a main class")
    require(path.exists(_.endsWith(".jar")), "JVM executor requires a JAR artifact")
    OperatorCommand(Some(Vector("/opt/java/bin/java", "--add-opens=java.base/java.nio=ALL-UNNAMED",
      "-Dio.netty.tryReflectionSetAccessible=true", "-cp", path.get + ":/opt/lib/*", p.entrypoint)))
  }
}
final class PythonExecutor extends OperatorExecutor {
  override def inferenceWorkerCommand(p:OperatorProgram,path:Option[String]):OperatorCommand=InferenceLaunch.python(command(p,path))
  override def sourceInferenceWorkerCommand(p:OperatorProgram,path:String):OperatorCommand=InferenceLaunch.python(sourceCommand(p,path))
  override def groupCommand(group:dacp.OperatorGroup):Option[OperatorCommand]=PythonGroup.command(group)
  val id = "python"
  override def sourceCommand(p: OperatorProgram, path: String): OperatorCommand =
    OperatorCommand(Some(Vector("/usr/local/bin/python","-c",PythonSource.launcher,path)),arguments=Vector("operator","/input","/input/params.json"),
      environment=Map("PYTHONDONTWRITEBYTECODE"->"1"))
  // Install the wheel offline into writable ephemeral storage, using dependencies supplied by the runtime.
  // pip validates wheel tags; its failure is fatal. No package index or network dependency resolution occurs.
  private val launcher = """import os,runpy,subprocess,sys
artifact,module=sys.argv[1:3]
args=sys.argv[3:]
if artifact.endswith('.whl'):
    subprocess.run([sys.executable,'-m','pip','install','--no-index','--no-deps','--no-compile','--target','/tmp/dacp-package',artifact],check=True,stdout=sys.stderr)
    sys.path.insert(0,'/tmp/dacp-package')
    sys.argv=[module]+args
    runpy.run_module(module,run_name='__main__')
elif artifact.endswith('.zip'):
    sys.path.insert(0,artifact)
    sys.argv=[module]+args
    runpy.run_module(module,run_name='__main__')
else:
    sys.path.insert(0,os.path.dirname(artifact))
    sys.argv=[artifact]+args
    runpy.run_path(artifact,run_name='__main__')
"""
  def command(p: OperatorProgram, path: Option[String]): OperatorCommand = {
    require(p.configuration.isEmpty,"executor does not accept configuration")
    require(path.exists(f => f.endsWith(".whl") || f.endsWith(".py") || f.endsWith(".zip")), "Python executor requires a wheel, source ZIP or Python source artifact")
    require(p.entrypoint.matches("[A-Za-z_][A-Za-z0-9_.]*"), "Python entrypoint must be a module name")
    OperatorCommand(Some(Vector("/usr/local/bin/python", "-c", launcher, path.get, p.entrypoint)),
      environment = Map("PYTHONDONTWRITEBYTECODE" -> "1", "PIP_DISABLE_PIP_VERSION_CHECK" -> "1"))
  }
}

/** Inline Python's process(inputs, params) yields bytes or Arrow RecordBatch objects on result. */
object PythonSource {
  val launcher = """import base64,json,struct,sys
import pyarrow as pa
import pyarrow.ipc as ipc

def frame(kind, data=b'', **meta):
    header=json.dumps(dict(type=kind,port='result',**meta)).encode()
    if len(header)>65536 or len(data)>8*1024*1024:
        raise ValueError('operator frame exceeds limits')
    sys.stdout.buffer.write(struct.pack('>II',len(header),len(data))+header+data)
    sys.stdout.buffer.flush()

def chunks(entry):
    with open('/input/'+entry['file'],'rb') as stream:
        with ipc.open_stream(stream) as reader:
            for batch in reader:
                if entry['representation']=='bytes':
                    for value in batch.column(0):
                        yield value.as_py()
                else:
                    yield batch

namespace={'__name__':'dacp_inline'}
with open(sys.argv[1]) as f:
    source=f.read()
protocol=sys.stdout
sys.stdout=sys.stderr
try:
    exec(compile(source,'<operator>','exec'),namespace)
finally:
    sys.stdout=protocol
if len(sys.argv)>2 and sys.argv[2]=='infer':
    from dacp_operator.inference import run_inference
    run_inference(namespace['infer_outputs'],sys.argv[2:],preview=namespace['preview_outputs'])
    sys.exit(0)
with open('/input/manifest.json') as f:
    manifest=json.load(f)
with open('/input/params.json') as f:
    params=json.load(f)
sys.stdout=sys.stderr
try:
    result=iter(namespace['process']({entry['port']:chunks(entry) for entry in manifest},params))
    first=next(result,None)
finally:
    sys.stdout=protocol
representation='records' if isinstance(first,pa.RecordBatch) else 'bytes'
meta=dict(representation=representation,format=params.get('output_format','binary'))
if representation=='records':
    meta['schema']=base64.b64encode(first.schema.serialize().to_pybytes()).decode()
frame('start',**meta)
value=first
while value is not None:
    if representation=='records':
        if not isinstance(value,pa.RecordBatch) or value.schema!=first.schema:
            raise ValueError('output schema changed')
        sink=pa.BufferOutputStream()
        with ipc.new_stream(sink,value.schema) as writer:
            writer.write_batch(value)
        frame('data',sink.getvalue().to_pybytes())
    else:
        if not isinstance(value,(bytes,bytearray,memoryview)):
            raise TypeError('process must yield bytes or RecordBatch')
        for offset in range(0,len(value),65536):
            frame('data',bytes(value[offset:offset+65536]))
    sys.stdout=sys.stderr
    try:
        value=next(result,None)
    finally:
        sys.stdout=protocol
frame('end')
"""
}

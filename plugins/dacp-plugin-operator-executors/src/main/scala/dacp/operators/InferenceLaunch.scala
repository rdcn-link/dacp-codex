package dacp.operators

import dacp.OperatorCommand

/** Worker adapters are runtime-specific; the core only manages bounded metadata channels. */
private[operators] object InferenceLaunch {
  private val arguments=Vector("infer-worker","/input/requests.fifo","/input/responses.fifo")
  def jvm(command:OperatorCommand):OperatorCommand={
    val entry=command.entrypoint.get;val cp=entry.indexOf("-cp");require(cp>=0,"JVM worker requires a classpath")
    command.copy(entrypoint=Some(entry.updated(cp+1,"/input/inference-worker.jar:"+entry(cp+1))),arguments=arguments,
      files=command.files+("inference-worker.jar"->JvmGroup.worker))
  }
  private def resource(name:String):Array[Byte]={val in=getClass.getResourceAsStream("/"+name);require(in!=null,"missing worker SDK resource");try in.readAllBytes()finally in.close()}
  // Bind the SDK inference entrypoint once. Function-style programs and template instances
  // use exactly the same worker loop, including wheels with a private bundled SDK.
  private val bootstrap="""import importlib.util,sys
_worker_args=sys.argv[-3:]
_spec=importlib.util.spec_from_file_location('dacp_operator','/input/worker-sdk.py',submodule_search_locations=['/input'])
_sdk=importlib.util.module_from_spec(_spec)
sys.modules['dacp_operator']=_sdk
_spec.loader.exec_module(_sdk)
_protocol=sys.modules['dacp_operator.inference']
def _serve(method,args=None,output=None,preview=None):
    return _protocol.serve_inference(method,_worker_args,preview=preview)
_protocol.run_inference=_serve
_sdk.run_inference=_serve
sys.argv[-3:]=['infer','/input/unused.json']
"""
  def python(command:OperatorCommand):OperatorCommand={
    val entry=command.entrypoint.get;val script=entry.indexOf("-c")+1;require(script>0,"Python worker requires a launcher")
    command.copy(entrypoint=Some(entry.updated(script,bootstrap+entry(script))),arguments=arguments,
      files=command.files++Map("worker-sdk.py"->resource("group-sdk.py"),"inference.py"->resource("inference.py")))
  }
}

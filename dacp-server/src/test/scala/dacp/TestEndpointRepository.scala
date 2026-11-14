package dacp

import xstream.data.Data
import java.net.InetSocketAddress
import java.security.MessageDigest
import com.sun.net.httpserver.HttpServer

/** Self-contained approved endpoint repository; no external service or container is required. */
final class TestEndpointRepository extends AutoCloseable {
  private val blobs=scala.collection.mutable.Map.empty[String,Array[Byte]]
  private val manifests=Seq("item_source"->"catalog.read","client_sink"->"client.receive").map { case(name,operation)=>
    val bytes=Data.mapper.createObjectNode().put("api_version",1).put("operation",operation).toString.getBytes("UTF-8")
    val hash=MessageDigest.getInstance("SHA-256").digest(bytes).map(b=>f"${b&255}%02x").mkString
    blobs(hash)=bytes
    val n=Data.mapper.createObjectNode().put("api_version",2).put("name",name).put("version","1.0").put("role",if(name=="item_source")"source"else"sink").put("type","sdk")
    n.putObject("icon").put("path","M3 5h18v14H3z").put("color","#287da2")
    val inputs=n.putObject("inputs")
    if(name=="client_sink"){val p=inputs.putObject("result");val a=p.putArray("representations");Seq("bytes","records","items","array","sequence").foreach(a.add);p.putArray("formats")}
    val params=n.putObject("parameters").put("type","object").put("additionalProperties",true);params.putObject("properties");params.putArray("required")
    n.putObject("runtime").put("image","sha256:"+"a"*64)
    n.putObject("artifact").put("sha256",hash).put("size",bytes.length).put("filename",name+".json")
    n.putObject("execution").put("executor","stream").put("entrypoint",operation).putObject("config")
    ("/v1/operators/"+name+"/1.0")->n.toString.getBytes("UTF-8")
  }.toMap
  private val server=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0)
  server.createContext("/",exchange=>{
    val path=exchange.getRequestURI.getPath
    val bytes=if(path=="/v1/runtimes")Some("[]".getBytes("UTF-8"))else if(path.startsWith("/v1/blobs/sha256/"))blobs.get(path.split('/').last)else manifests.get(path)
    val response=bytes.getOrElse(Array.emptyByteArray)
    exchange.sendResponseHeaders(if(bytes.isDefined)200 else 404,response.length.toLong)
    try exchange.getResponseBody.write(response)finally exchange.close()
  })
  server.start()
  val url="http://127.0.0.1:"+server.getAddress.getPort
  def close():Unit=server.stop(0)
}

package dacp

import xstream.data._
import xstream.data.files.{FileDatasets,PublishedItem}
import xstream.engine.{TaskExecutor,Jobs}
import xstream.execution.{OperatorFrame,StreamCancel,StreamProtocol}
import java.nio.file.Files
import java.util.zip.{ZipEntry,ZipOutputStream}
import org.apache.arrow.memory.RootAllocator
import org.scalatest.funsuite.AnyFunSuite

class ZipItemSourceSpec extends AnyFunSuite {
  test("archive bytes support authenticated metadata inference, remote and context item_source execution") {
    val dir=Files.createTempDirectory("zip-read-item-")
    val archive=dir.resolve("geotrans-input.zip");val zip=new ZipOutputStream(Files.newOutputStream(archive))
    try {zip.putNextEntry(new ZipEntry("labels/points.txt"));zip.write("points".getBytes("UTF-8"));zip.closeEntry()}finally zip.close()
    val plain=dir.resolve("plain.txt");Files.writeString(plain,"plain")
    val socket=new java.net.ServerSocket(0);val port=socket.getLocalPort;socket.close()
    val repository=new TestEndpointRepository()
    val config=Config("localhost",port,"localhost",Map("files"->SecurityTestSupport.publicFiles(Seq(
      PublishedItem("/geotrans-input.zip",file=Some(archive)),PublishedItem("/plain.txt",file=Some(plain))))),
      SecurityTestSupport.certificate,SecurityTestSupport.key,SecurityTestSupport.certificate,
      remoteOperatorLibraryUrls=Seq(repository.url),operatorCache=dir.resolve("operators"),
      credentialDirectory=Some(dir.resolve("credentials")),accessLogDirectory=Some(dir.resolve("access")),
      web=NodeWebSettings(enabled=false),s3=S3Settings(enabled=false))
    val server=DacpServer.builder().config(config).build().start();val allocator=new RootAllocator()
    // A different local origin forces ItemSource through the authenticated remote transport branch.
    val token=dir.resolve("reader.token");Files.writeString(token,config.internalToken)
    val remote=config.copy(port=if(port==65535)port-1 else port+1,remoteItemCredentials=Map(DacpUri.origin(config.uri(""))->token))
    def task(uri:String,view:String)=Jobs.parse(s"name: ZIP\nsteps:\n- id: read\n  operator: item_source@1.0\n  inputs: {}\n  params: {uri: '$uri', view: $view}\n- id: client_sink\n  operator: client_sink@1.0\n  inputs: {result: read}\n").steps.head
    def read(c:Config,uri:String,view:String):Array[Byte]={
      val cancel=new StreamCancel();val out=new java.io.ByteArrayOutputStream()
      try {new TaskExecutor(c,allocator).execute(task(uri,view),Map.empty,f=>if(StreamProtocol.header(f).path("type").asText=="data")out.write(f.data),cancel);out.toByteArray}finally cancel.close()
    }
    try {
      for((path,expected)<-Seq("plain.txt"->"plain".getBytes("UTF-8"),"geotrans-input.zip"->Files.readAllBytes(archive),"geotrans-input.zip/labels/points.txt"->"points".getBytes("UTF-8"))) {
        val uri=config.uri("files")+"/"+path
        val cancel=new StreamCancel()
        try {
          val result=WorkflowInference.task(remote,allocator,task(uri,"bytes"),Map.empty,cancel).outputs("result")
          assert(result.representations==Set("bytes") && result.certainty=="known")
        }finally cancel.close()
        val provider=new xstream.connector.dacp.DacpRemoteItemProvider();provider.initialize(config.plugins)
        val settings=Data.mapper.createObjectNode()
        settings.putArray("nodes").addObject().put("url",DacpUri.origin(config.uri(""))).put("ca",config.ca.toString).put("token_file",token.toString)
        val remoteCancel=new StreamCancel();val remoteBytes=new java.io.ByteArrayOutputStream()
        try {
          val source=provider.open(settings,xstream.execution.RemoteItemRequest(uri,"bytes",None,None),allocator,remoteCancel)
          try {var next=source.next();while(next.nonEmpty){if(StreamProtocol.header(next.get).path("type").asText=="data")remoteBytes.write(next.get.data);next=source.next()}}finally source.close()
          assert(remoteBytes.toByteArray.sameElements(expected))
        }finally remoteCancel.close()
        assert(read(remote,uri,"bytes").sameElements(expected))
        assert(read(config,uri,"bytes").sameElements(expected))
        assert(read(config,"context:/files/"+path,"bytes").sameElements(expected))
      }
      // Item references retain the remote origin at the Flight boundary.
      val itemProvider=new xstream.connector.dacp.DacpRemoteItemProvider();itemProvider.initialize(config.plugins)
      val itemSettings=Data.mapper.createObjectNode()
      itemSettings.putArray("nodes").addObject().put("url",DacpUri.origin(config.uri(""))).put("ca",config.ca.toString).put("token_file",token.toString)
      val itemCancel=new StreamCancel()
      val members=scala.collection.mutable.ArrayBuffer.empty[ItemReference]
      try {
        val source=itemProvider.open(itemSettings,xstream.execution.RemoteItemRequest(config.uri("files")+"/geotrans-input.zip","items",None,Some(Data.mapper.createObjectNode().put("recursive",true))),allocator,itemCancel)
        try {
          var next=source.next()
          while(next.nonEmpty) {
            val frame=next.get
            if(StreamProtocol.header(frame).path("type").asText=="data")
              ItemReferences.foreach(ArrowIpc.readStream(new java.io.ByteArrayInputStream(frame.data),StreamContext(allocator)))(members+=_)
            next=source.next()
          }
        }finally source.close()
        assert(members.exists(_.uri==config.uri("files")+"/geotrans-input.zip/labels/points.txt"))
        assert(members.exists(i=>i.kind=="folder" && i.views.exists(_.contains("items"))))
      }finally itemCancel.close()
      config.credentials.setRoles("zip-runner",Set("runner"))
      val runner=config.credentials.create("zip-runner");val runnerToken=dir.resolve("runner.token");Files.writeString(runnerToken,runner._2)
      val job="name: Submitted ZIP read\nsteps:\n- id: read\n  operator: item_source@1.0\n  inputs: {}\n  params: {uri: context:/files/geotrans-input.zip, view: bytes}\n- id: client_sink\n  operator: client_sink@1.0\n  inputs: {result: read}\n"
      val submitted=dacp.client.Job.run(config.uri(""),job,dacp.client.TlsConfig(ca=Some(config.ca),tokenFile=Some(runnerToken)),30)
      val submittedBytes=new java.io.ByteArrayOutputStream()
      try {
        while(submitted.hasNext){val event=submitted.next();if(event.kind=="data")submittedBytes.write(event.data)}
        assert(submitted.executionId.nonEmpty)
        assert(submittedBytes.toByteArray.sameElements(Files.readAllBytes(archive)))
      }finally submitted.close()
      for((path,view)<-Seq("geotrans-input.zip/labels"->"bytes","geotrans-input.zip"->"records")) {
        val uri=config.uri("files")+"/"+path
        val cancel=new StreamCancel()
        try intercept[Exception](WorkflowInference.task(remote,allocator,task(uri,view),Map.empty,cancel))finally cancel.close()
        intercept[Exception](read(remote,uri,view))
        intercept[Exception](read(config,"context:/files/"+path,view))
      }
    }finally {remote.close();server.close();repository.close();allocator.close();Data.deleteTree(dir)}
  }
}

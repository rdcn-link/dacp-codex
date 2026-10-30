package xstream.designer

import _root_.xstream.data._
import dacp.union.Json
import org.scalatest.funsuite.AnyFunSuite
import java.nio.file.Files
import java.net.URI
import java.net.http.{HttpClient,HttpRequest,HttpResponse}

class ScientificDesignerSpec extends AnyFunSuite {
  test("scientific catalogs, plugin selection forms, bounded preview and YAML export work without workers") {
    val dir=Files.createTempDirectory("scientific-designer-")
    Files.writeString(dir.resolve("sample.fa"),">chr1 example\nACGTACGT\n>chr2\nNNNN\n")
    val nc=ucar.nc2.write.NetcdfFormatWriter.createNewNetcdf3(dir.resolve("climate.nc").toString)
    nc.addDimension("lat",2);nc.addDimension("lon",3);nc.addVariable("temperature",ucar.ma2.DataType.INT,"lat lon")
    val writer=nc.build();try writer.write("temperature",ucar.ma2.Array.factory(ucar.ma2.DataType.INT,Array(2,3),Array(1,2,3,4,5,6)))finally writer.close()
    val fits=new nom.tam.fits.Fits();fits.addHDU(nom.tam.fits.Fits.makeHDU(Array(Array[Short](1,2,3),Array[Short](4,5,6))));fits.write(dir.resolve("sky.fits").toFile);fits.close()
    val config=Documents.mapping(s"""host: 127.0.0.1
      |port: 0
      |repository: {url: 'http://127.0.0.1:1'}
      |operator_cache: '${dir.resolve("cache")}'
      |local_catalog:
      |  datasets:
      |    science:
      |      provider: directory
      |      source: {root: '$dir'}
      |""".stripMargin)
    val site=new Designer(config);site.start();val http=HttpClient.newHttpClient()
    def post(path:String,n:com.fasterxml.jackson.databind.JsonNode):com.fasterxml.jackson.databind.JsonNode={
      val r=http.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${site.address.getPort}/api/$path")).header("Content-Type","application/json").POST(HttpRequest.BodyPublishers.ofString(n.toString)).build(),HttpResponse.BodyHandlers.ofString())
      assert(r.statusCode()==200,r.body());Json.mapper.readTree(r.body())
    }
    def job(view:String="sequence",selection:String="{ids: [chr1], offset: 2, length: $N}")=s"""name: Scientific preview
      |tasks:
      |- id: read
      |  operator: read_item@1.0
      |  inputs: {}
      |  params:
      |    uri: context:/science/sample.fa/sequences
      |    view: $view
      |    selection: $selection
      |outputs: {result: read}
      |""".stripMargin
    try {
      val views=post("views",Json.obj());assert(views.path("array").path("selection").path("properties").has("stride"));assert(views.path("image").path("selection").path("properties").has("width"));assert(views.path("sequence").path("selection").path("properties").has("ids"))
      val asset=http.send(HttpRequest.newBuilder(URI.create(s"http://127.0.0.1:${site.address.getPort}/scientific-views.js")).GET().build(),HttpResponse.BodyHandlers.ofString());assert(asset.statusCode()==200)
      val children=post("items",Json.obj().put("uri","context:/science").put("path","/sample.fa"));assert(children.path("items").get(0).path("views").get(0).asText=="sequence")
      val request=Json.obj().put("job_yaml",job()).put("variables_yaml","N: 4")
      val plain=post("infer",request);val d=plain.path("tasks").path("read").path("outputs").path("result")
      assert(d.path("certainty").asText=="known" && d.path("structure").path("view").asText=="sequence" && !d.has("sample"))
      request.set[com.fasterxml.jackson.databind.JsonNode]("preview",Json.obj().put("max_rows",2).put("max_bytes",1024))
      val sample=post("infer",request).path("outputs").path("result").path("sample")
      assert(sample.path("representation").asText=="sequence" && sample.path("rows").get(0).path("symbols").asText=="GTAC")
      assert(sample.path("rows").get(0).path("offset").asLong==2)
      assert(Json.mapper.writeValueAsBytes(sample).length<=1024)
      val variants=Vector(
        ("array","climate.nc/temperature","{origin: [0, 1], shape: [2, 1], stride: [1, 1]}"),
        ("image","sky.fits/hdu0","{x: 1, y: 0, width: 1, height: 2}"))
      variants.foreach {case(view,path,selection)=>
        val yaml=job(view,selection).replace("sample.fa/sequences",path)
        val r=post("infer",Json.obj().put("job_yaml",yaml).put("variables_yaml","{}").set[com.fasterxml.jackson.databind.JsonNode]("preview",Json.obj().put("max_rows",2).put("max_bytes",1024)))
        val out=r.path("outputs").path("result");assert(out.path("structure").path("view").asText==view,r.toString)
        val row=out.path("sample").path("rows").get(0)
        assert(row.path("values").get(0).asInt==2 && row.path("values").get(1).asInt==5)
        if(view=="array")assert(out.path("structure").path("dimensions").size==2)
        else assert(out.path("structure").path("width").asInt==3 && row.path("x").asInt==1)
      }
      val exported=post("export",request).path("job_yaml").asText
      assert(Documents.job(exported).path("tasks").get(0).path("params").path("selection").path("length").asText=="$N")
      request.put("job_yaml",job(selection="{length: -1}"))
      val invalid=post("infer",request);assert(invalid.path("tasks").path("read").path("diagnostics").get(0).path("severity").asText=="error")
    }finally {site.close();Data.deleteTree(dir)}
  }
}

package dacp

import java.nio.file.{Files, Path}
import java.nio.charset.StandardCharsets.UTF_8
import scala.jdk.CollectionConverters._
import org.apache.arrow.vector._
import org.apache.arrow.vector.types.pojo.{ArrowType, Field, FieldType, Schema}
import org.apache.arrow.memory.BufferAllocator

class DirectoryAdapter(base: Path, batchSize: Int, inferSample: Int, excludeMetadata: Boolean = false, val options: FileOptions = FileOptions(), byteChunk: Int = 65536, plugins:PluginEnvironment) extends DatasetAccess {
  private val Formats=plugins.formats
  private val root = base.toRealPath()
  def resolve(item: String): Path = {
    require(item.nonEmpty && !Path.of(item).isAbsolute && !item.split("/", -1).exists(x => x == ".." || x == "." || x.isEmpty), "invalid item path")
    val p = root.resolve(item).toRealPath()
    require(p.startsWith(root) && Files.isRegularFile(p), "item escapes dataset or is not a file")
    p
  }
  def describe(item: String): Item = {
    val p = resolve(item); val handler = Formats.detect(ByteSource.local(p),options)
    Item(item,handler.id,Some(if (handler.isInstanceOf[StructuredFileHandler]) PluginSchemas.bytes else handler.schema(ByteSource.local(p),options,batchSize,inferSample)))
  }
  def list(path: String): Seq[Item] = {
    require(path == "/", "listing requires root")
    list()
  }
  def count(item: String): Option[Long] = {
    val path = resolve(item)
    Formats.detect(ByteSource.local(path),options).count(ByteSource.local(path),options)
  }
  override def scan(item: String, request: Scan, allocator: BufferAllocator,
                    listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener, ready: () => Unit): Unit = {
    val path = resolve(item)
    Formats.detect(ByteSource.local(path),options).scan(ByteSource.local(path),options,request,allocator,batchSize,inferSample,listener,ready)
  }
  def stream(item: String, view: String, allocator: BufferAllocator,
             listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener, ready: () => Unit): Unit = {
    require(Set("records","bytes")(view), "invalid view")
    ready()
    val path = resolve(item)
    val handler = Formats.detect(ByteSource.local(path),options)
    require(view == "bytes" || handler.records, "unsupported stream view")
    if (view == "bytes") {
      val root = VectorSchemaRoot.create(PluginSchemas.bytes,allocator)
      try {
        val in = Files.newInputStream(path)
        try {
          listener.start(root)
          val buffer = new Array[Byte](byteChunk)
          var done = false
          while (!done) {
            ready(); val n = in.read(buffer)
            if (n < 0) done = true else {
              root.clear(); root.allocateNew()
              root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,buffer,0,n)
              root.setRowCount(1); listener.putNext()
            }
          }
        } finally in.close()
      } finally root.close()
    } else handler.stream(ByteSource.local(path),options,allocator,batchSize,inferSample)(
      root => listener.start(root), _ => listener.putNext(), ready)
    listener.completed()
  }
  // Architecture: recurse and flatten files, never return directories or follow escaping symlinks.
  def list(): Seq[Item] = {
    val walk = Files.walk(root)
    try walk.iterator.asScala.filter(p => Files.isRegularFile(p) && (!excludeMetadata || p.getFileName.toString != "_schema.json") && p.toRealPath().startsWith(root))
      .map { p => val handler = Formats.detect(ByteSource.local(p),options); Item(root.relativize(p).toString.replace('\\','/'), handler.id, handler.cheapSchema(ByteSource.local(p),options)) }.toSeq.sortBy(_.path)
    finally walk.close()
  }
}

package dacp

import java.nio.file.Path
import scala.jdk.CollectionConverters._
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema

/** Private source options never travel in public metadata or tickets. */
case class FileOptions(format: Option[String] = None, columns: Option[Schema] = None,
                       parameters: Map[String,Object] = Map.empty) {
  columns.foreach { schema =>
    val names = schema.getFields.asScala.map(_.getName)
    require(names.nonEmpty && names.forall(_.nonEmpty) && names.distinct.size == names.size, "schema requires unique nonempty column names")
  }
  override def toString: String = "FileOptions(<private settings>)"
}

/** Implementations must release resources on close and honor demand before each batch. */
trait FormatHandler extends Plugin {
  def id: String
  def extensions: Set[String]
  def records: Boolean = false
  def validateOptions(options: FileOptions): Unit =
    require(options.parameters.isEmpty, "this format does not accept reader parameters")
  def signatureMatches(header: Array[Byte]): Boolean = false
  def matches(header: Array[Byte], extension: String): Boolean = extensions(extension)
  def rows(path: ByteSource, options: FileOptions): Records.Rows = throw new IllegalArgumentException("unsupported stream view")
  def cheapSchema(path: ByteSource, options: FileOptions): Option[Schema] = None
  def cheapCount(path: ByteSource, options: FileOptions): Option[Long] = None
  def schema(path: ByteSource, options: FileOptions, batch: Int, sample: Int): Schema =
    if (!records) PluginSchemas.bytes else Records.inferSchema(rows(path,options),batch,sample,options.columns)
  def count(path: ByteSource, options: FileOptions): Option[Long] = if (!records) None else {
    val reader = rows(path,options)
    try Some(reader.iterator.foldLeft(0L)((n,_) => Math.addExact(n,1L))) finally reader.close()
  }
  /** Optional source optimization; default preserves semantics with bounded batch filtering. */
  def scan(path: ByteSource, options: FileOptions, request: Scan, allocator: BufferAllocator, batch: Int, sample: Int,
           listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener, demand: () => Unit): Unit = {
    require(records,"scan options require records view")
    ScanStreams.stream(request,allocator,listener) { filtered =>
      stream(path,options,allocator,batch,sample)(root => filtered.start(root),_ => filtered.putNext(),demand)
    }
  }
  def stream(path: ByteSource, options: FileOptions, allocator: BufferAllocator, batch: Int, sample: Int)(
      start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit = {
    val source=ByteSource.withCheck(path,demand)
    val root = VectorSchemaRoot.create(schema(source,options,batch,sample),allocator)
    try {
      val reader = rows(source,options)
      try {
        start(root)
        val iterator = reader.iterator
        var done = false
        while (!done) {
          demand()
          val values = iterator.take(batch).toVector
          if (values.isEmpty) done = true else { Records.fill(root,values); emit(root) }
        }
      } finally reader.close()
    } finally root.close()
  }
}

/** Native logical objects inside a file; children need not be extracted as files. */
trait StructuredFileHandler extends FormatHandler {
  def openStructure(path: ByteSource, options: FileOptions, context: DatasetContext,
                    check: () => Unit): DatasetAccess
}

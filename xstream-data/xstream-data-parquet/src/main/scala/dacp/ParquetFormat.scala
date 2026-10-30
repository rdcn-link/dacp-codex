package dacp

import _root_.xstream.data._
import java.nio.file.Path
import org.apache.arrow.memory.BufferAllocator
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.types.pojo.Schema
class ParquetFormat extends FormatHandler {
    val id = "parquet"; val extensions = Set("parquet")
    override def signatureMatches(header: Array[Byte]): Boolean = header.take(4).sameElements(Array[Byte](80,65,82,49))
    override val records = true
    override def cheapSchema(p: ByteSource, o: FileOptions) = Some(Parquet.schema(p))
    override def cheapCount(p: ByteSource, o: FileOptions) = Some(Parquet.recordCount(p))
    override def schema(p: ByteSource, o: FileOptions, b: Int, s: Int): Schema = {
      val schema = Parquet.schema(p)
      require(o.columns.forall(_ == schema), "SCHEMA_CONFLICT: configured schema differs from Parquet metadata")
      schema
    }
    override def scan(p: ByteSource, o: FileOptions, request: Scan, a: BufferAllocator, b: Int, s: Int,
                      listener: BatchReceiver, demand: () => Unit): Unit = {
      schema(ByteSource.withCheck(p,demand),o,b,s)
      ScanStreams.stream(request,a,listener) { filtered =>
        Parquet.stream(p,a,b,Some(request))(root => filtered.start(root),_ => filtered.putNext(),demand)
      }
    }
    override def count(p: ByteSource, o: FileOptions) = cheapCount(p,o)
    override def stream(p: ByteSource, o: FileOptions, a: BufferAllocator, b: Int, s: Int)(
        start: VectorSchemaRoot => Unit, emit: VectorSchemaRoot => Unit, demand: () => Unit): Unit =
      Parquet.stream(p,a,b)(start,emit,demand)
  }

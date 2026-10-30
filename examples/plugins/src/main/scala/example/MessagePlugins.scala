package example

import _root_.xstream.data._

import dacp._
import java.nio.file.Path
import java.nio.charset.StandardCharsets.UTF_8
import org.apache.arrow.memory.BufferAllocator
import _root_.xstream.data.BatchReceiver
import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}

/** A native dataset, with no dependency on the server implementation or a physical file. */
class MessageProvider extends DatasetProvider {
  val id = "example-message"
  def create(d: DatasetDefinition, context: DatasetContext): PublishedDataset = {
    require(d.source.keySet == Set("message") && d.source("message").isInstanceOf[String],"message must be a string")
    require(d.items.isEmpty,"message provider has no item mappings")
    val message = d.source("message").toString
    new PublishedDataset {
      def open(check: () => Unit): DatasetAccess = new MessageAccess(() => message,check)
    }
  }
}

/** A structured file whose internal object is streamed directly, without extraction. */
class MessageFileHandler extends StructuredFileHandler {
  val id = "example-message-file"
  val extensions = Set("message")
  override def validateOptions(o: FileOptions): Unit = {
    require(o.parameters.keySet.subsetOf(Set("prefix")),"unknown message reader parameter")
    require(o.parameters.get("prefix").forall(_.isInstanceOf[String]),"prefix must be a string")
  }
  def openStructure(path: ByteSource, options: FileOptions, context: DatasetContext, check: () => Unit): DatasetAccess = {
    validateOptions(options)
    new MessageAccess(() => options.parameters.getOrElse("prefix","").toString + scala.util.Using.resource(path.open(check))(in=>new String(in.readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)),check)
  }
}

/** Small example only: production handlers should stream large payloads in bounded chunks. */
class MessageAccess(message: () => String, check: () => Unit) extends ItemDatasetAccess {
  private val item = Item("/message","text",Some(PluginSchemas.bytes),views=Some(Seq("bytes")))
  private val summarySchema = new org.apache.arrow.vector.types.pojo.Schema(
    java.util.Collections.singletonList(Records.field("characters","int64")))
  private val summary = Item("/summary","message-summary",Some(summarySchema),views=Some(Seq("records")))
  def list(): Seq[Item] = { check(); Seq(item,summary) }
  def list(path: String): Seq[Item] = {
    describe(path); require(path == "/","listing requires a folder"); list()
  }
  def describe(path: String): Item = {
    check(); LogicalPath.validate(path)
    if (path == "/") Item("/","",None,"folder",views=Some(Seq.empty))
    else if (path == item.path) item
    else if (path == summary.path) summary
    else throw new java.io.FileNotFoundException("logical item not found")
  }
  def openItem(path: String): ItemAccess = {
    val selected = describe(path)
    require(selected.kind == "data", "folder has no content stream")
    new ItemAccess {
      def count(): Option[Long] = if (path == summary.path) Some(1L) else None
      def view[K,S](kind:DataViewType[K,S]):Option[DataView[K,S]] = {
        if(path==item.path) DataViews.when(kind,DataViews.Bytes)(
          DataViews[ByteChunk,AllBytes.type]((_,context,out)=>{
            context.check();val bytes=content().getBytes(UTF_8)
            out.start(BytesDescriptor("text/plain; charset=utf-8"));context.check()
            out.accept(ByteChunk(bytes,0,bytes.length))
          }))
        else DataViews.when(kind,DataViews.Records)(
          DataViews[RecordBatch,Scan]((scan,context,out)=>{
            context.check();val text=content()
            ScanStreams.stream(scan,context.allocator,BatchStreams.records(out,context.check)){ receiver=>
              val root=VectorSchemaRoot.create(summarySchema,context.allocator)
              try {
                receiver.start(root);context.check()
                Records.fill(root,Seq(Seq(text.codePointCount(0,text.length).toLong)))
                receiver.putNext()
              } finally root.close()
            }
          }))
      }
    }
  }
  private def content():String=try message() catch {
    case scala.util.control.NonFatal(_) => throw new FormatFailure("Cannot read message content.")
  }
}

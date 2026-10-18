package dacp

import org.apache.arrow.memory.{ArrowBuf, BufferAllocator}
import org.apache.arrow.vector.VectorSchemaRoot
import org.apache.arrow.vector.dictionary.DictionaryProvider
import org.apache.arrow.vector.ipc.message.IpcOption
import org.apache.arrow.flight.FlightProducer.ServerStreamListener

/** Batch-bounded fallback. Providers may override scan to execute equivalent source operations. */
object ScanStreams {
  private class LimitReached extends RuntimeException(null,null,false,false)
  def stream(scan: Scan, allocator: BufferAllocator, listener: ServerStreamListener)(
      read: ServerStreamListener => Unit): Unit = {
    var input: VectorSchemaRoot = null
    var output: VectorSchemaRoot = null
    var bound: BoundScan = null
    var remaining = scan.limit.getOrElse(Long.MaxValue)
    val wrapped = new ServerStreamListener {
      override def isReady: Boolean = listener.isReady
      override def isCancelled: Boolean = listener.isCancelled
      override def setOnReadyHandler(r: Runnable): Unit = listener.setOnReadyHandler(r)
      override def setOnCancelHandler(r: Runnable): Unit = listener.setOnCancelHandler(r)
      override def start(root: VectorSchemaRoot, dictionaries: DictionaryProvider, option: IpcOption): Unit = {
        require(dictionaries == null,"dictionary-encoded records scans are unsupported")
        input = root; bound = scan.bind(root.getSchema)
        output = VectorSchemaRoot.create(bound.outputSchema,allocator)
        listener.start(output,null,option)
        if(remaining == 0) throw new LimitReached
      }
      override def putNext(): Unit = {
        output.clear(); output.allocateNew()
        var written = 0; var row = 0
        while(row < input.getRowCount && remaining > 0) {
          if(bound.matches(input,row)) {
            bound.indices.zipWithIndex.foreach { case (source,target) =>
              output.getVector(target).copyFromSafe(row,written,input.getVector(source))
            }
            written += 1; remaining -= 1
          }
          row += 1
        }
        if(written > 0) { output.setRowCount(written); listener.putNext() }
        if(remaining == 0) throw new LimitReached
      }
      override def putNext(metadata: ArrowBuf): Unit = {
        require(metadata == null,"records scan does not support batch metadata"); putNext()
      }
      override def putMetadata(metadata: ArrowBuf): Unit = listener.putMetadata(metadata)
      override def completed(): Unit = ()
      override def error(t: Throwable): Unit = throw t
    }
    try {
      try read(wrapped) catch { case _: LimitReached => () }
      listener.completed()
    } finally if(output != null) output.close()
  }
}

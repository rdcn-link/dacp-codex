package dacp

import _root_.xstream.execution.{OperatorFrame,OperatorFrames,StreamCancel}

import org.apache.arrow.vector.{VectorSchemaRoot,VarBinaryVector}

/** Flight transport belongs to DACP, not the execution contracts. */
object FlightFrames {
  def send(listener: org.apache.arrow.flight.FlightProducer.ServerStreamListener,root: VectorSchemaRoot,f: OperatorFrame,cancel: StreamCancel): Unit = {
    while(!listener.isReady){cancel.active();Thread.sleep(10)};cancel.active()
    root.clear();root.allocateNew();root.getVector(0).asInstanceOf[VarBinaryVector].setSafe(0,OperatorFrames.encode(f));root.setRowCount(1);listener.putNext()
  }
}

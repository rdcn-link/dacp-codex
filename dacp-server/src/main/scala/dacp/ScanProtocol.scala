package dacp

import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

object ScanProtocol {
  def encode(scan:Scan):Metadata.Scan={
    def predicate(p:Predicate):Metadata.Predicate={
      val b=Metadata.Predicate.newBuilder().setOp(p.op).setColumn(p.column)
      p.values.foreach(v=>b.addValues(Metadata.Literal.newBuilder().setType(v.kind).setValue(v.value)))
      p.children.foreach(c=>b.addChildren(predicate(c)));b.build()
    }
    val b=Metadata.Scan.newBuilder()
    scan.predicate.foreach(p=>b.setPredicate(predicate(p)))
    scan.columns.foreach(c=>b.setProjection(Metadata.Scan.Projection.newBuilder().addAllColumns(c.asJava)))
    scan.limit.foreach(b.setLimit);b.build()
  }

  def decode(s: Metadata.Scan): Scan = ScanFailure.validate {
    var nodes = 0
    def predicate(p: Metadata.Predicate,depth: Int): Predicate = {
      nodes += 1
      require(depth <= 32 && nodes <= 1024,"predicate is too complex")
      Predicate(p.getOp,p.getColumn,p.getValuesList.asScala.map(l => Literal(l.getType,l.getValue)).toVector,
        p.getChildrenList.asScala.map(predicate(_,depth+1)).toVector)
    }
    Scan(if(s.hasPredicate) Some(predicate(s.getPredicate,0)) else None,
      if(s.hasProjection) Some(s.getProjection.getColumnsList.asScala.toVector) else None,
      if(s.hasLimit) Some(s.getLimit) else None)
  }
}

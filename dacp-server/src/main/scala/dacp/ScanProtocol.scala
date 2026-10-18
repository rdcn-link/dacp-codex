package dacp

import dacp.protocol.Metadata
import scala.jdk.CollectionConverters._

object ScanProtocol {
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

package dacp

import org.apache.arrow.flight.{FlightProducer,FlightServerMiddleware}
private[dacp] object TestCalls {
  val context:FlightProducer.CallContext=new FlightProducer.CallContext {
    def peerIdentity():String="test"
    def isCancelled():Boolean=false
    def getMiddleware[T <: FlightServerMiddleware](key:FlightServerMiddleware.Key[T]):T=null.asInstanceOf[T]
    def getMiddleware():java.util.Map[FlightServerMiddleware.Key[_],FlightServerMiddleware]=java.util.Collections.emptyMap()
  }
}

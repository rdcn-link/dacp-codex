package xstream.data

/** Shared metadata policy for source inference, execution and authoring. */
object ItemViews {
  def select(requested:String,available:Iterable[String]):String={
    val views=available.toSet
    val view=if(requested=="auto"){
      if(views("records"))"records"else if(views("bytes"))"bytes"else available.headOption.getOrElse(throw new IllegalArgumentException("Item has no readable view"))
    }else requested
    require(views(view),"Item does not support requested view: "+view)
    view
  }
}

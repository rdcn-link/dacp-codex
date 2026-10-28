package dacp

/** Shared metadata policy for source inference, execution and authoring. */
object ItemViews {
  def select(requested:String,available:Iterable[String]):String={
    val views=available.toSet
    val view=if(requested=="auto"){
      if(views("records"))"records"else "bytes"
    }else requested
    require(Set("bytes","records")(view) && views(view),"Item does not support requested view: "+view)
    view
  }
}

package dacp

/** Reusable metadata contract checks for plugin authors' tests. Does not read or count content. */
object PluginChecks {
  def listing(access: DatasetAccess): Unit = {
    val all = access.list()
    require(all.map(_.path).distinct.size == all.size,"duplicate plugin item path")
    all.foreach { item =>
      LogicalPath.validate(item.path)
      require(item.path != "/","listing includes root")
      require(Set("data","folder")(item.kind),"invalid item kind")
      require(item.totalSizeBytes.forall(_ >= 0) && item.recordCount.forall(_ >= 0),"negative item properties")
      if (item.kind == "folder")
        require(item.lastModifiedTimeMs.isEmpty && item.totalSizeBytes.isEmpty && item.recordCount.isEmpty,"folder properties must be unknown")
      require(access.describe(item.path).path == item.path,"describe changed logical path")
    }
    val byPath = all.map(i => i.path -> i).toMap
    all.foreach { item =>
      val parent = LogicalPath.parent(item.path)
      require(parent == "/" || byPath.get(parent).exists(_.kind == "folder"),"missing parent folder")
    }
    (Seq("/") ++ all.filter(_.kind == "folder").map(_.path)).foreach { parent =>
      val expected = all.filter(i => LogicalPath.parent(i.path) == parent).map(_.path).toSet
      val children = access.list(parent)
      require(children.map(_.path).distinct.size == children.size,"duplicate immediate children")
      require(children.map(_.path).toSet == expected,"list(path) must return immediate children")
    }
  }
}

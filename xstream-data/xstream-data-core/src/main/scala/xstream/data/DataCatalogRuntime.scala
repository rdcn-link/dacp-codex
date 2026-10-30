package xstream.data

/** One process-wide catalog. Hosts acquire leases; the final owner closes the catalog. */
object DataCatalogRuntime {
  private var current:DataCatalog=null
  private var leases=0
  def catalog:DataCatalog=synchronized {require(current!=null,"DataCatalog has not been initialized");current}
  def bind(value:DataCatalog):AutoCloseable=synchronized {
    require(value!=null,"DataCatalog required")
    require(current==null || (current eq value),"This JVM already has a different DataCatalog")
    current=value;leases+=1
    new AutoCloseable {
      private var closed=false
      def close():Unit=DataCatalogRuntime.synchronized {
        if(!closed){closed=true;leases-=1;if(leases==0){val old=current;current=null;old.close()}}
      }
    }
  }
}

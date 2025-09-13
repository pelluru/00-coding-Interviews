package problems
object Problem098DesignCacheTtl {
  class TTLCache[K,V] {
    private val store = scala.collection.mutable.HashMap[K,(V,Long)]()
    def put(k:K, v:V, ttlMillis:Long): Unit = store(k)=(v, System.currentTimeMillis()+ttlMillis)
    def get(k:K): Option[V] = store.get(k).flatMap{case (v,exp) => if (System.currentTimeMillis()<=exp) Some(v) else { store.remove(k); None } }
    def size: Int = store.size
  }
}

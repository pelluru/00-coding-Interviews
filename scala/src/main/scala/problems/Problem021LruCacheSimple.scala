package problems
object Problem021LruCacheSimple {
  class LRU[K,V](cap:Int) extends java.util.LinkedHashMap[K,V](16,0.75f,true) {
    override def removeEldestEntry(e: java.util.Map.Entry[K,V]): Boolean = this.size() > cap
  }
}

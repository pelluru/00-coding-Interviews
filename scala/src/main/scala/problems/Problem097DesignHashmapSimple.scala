package problems
object Problem097DesignHashmapSimple {
  class MyHashMap[K,V](cap:Int=1024) {
    private case class Node(k:K, var v:V, var next:Node)
    private val buckets = new Array[Node](cap)
    private def idx(k:K) = (k.hashCode & 0x7fffffff) % cap
    def put(k:K, v:V): Unit = {
      val i = idx(k)
      var n = buckets(i)
      while (n!=null) { if (n.k==k) { n.v=v; return }; n=n.next }
      val newN = Node(k,v,buckets(i)); buckets(i)=newN
    }
    def get(k:K): Option[V] = {
      var n = buckets(idx(k)); while (n!=null) { if (n.k==k) return Some(n.v); n=n.next }; None
    }
    def remove(k:K): Unit = {
      val i=idx(k)
      var n=buckets(i); var prev:Node=null
      while (n!=null) { if (n.k==k) { if (prev==null) buckets(i)=n.next else prev.next=n.next; return }; prev=n; n=n.next }
    }
  }
}


package problems

object Problem036SerializeDeserializeTree {
  final class Node(var v:Int, var l:Node, var r:Node)
  object Node { def apply(v:Int): Node = new Node(v, null, null) }

  def serialize(root: Node): String = {
    val sb = new StringBuilder
    def ser(n:Node): Unit = {
      if (n==null) { sb.append("#,"); return }
      sb.append(n.v).append(','); ser(n.l); ser(n.r)
    }
    ser(root); sb.toString
  }
  def deserialize(data:String): Node = {
    val t = data.split(",")
    val idx = Array(0)
    def des(): Node = {
      if (idx(0) >= t.length) return null
      val v = t(idx(0)); idx(0) += 1
      if (v=="" || v=="#") return null
      val n = Node(v.toInt); n.l = des(); n.r = des(); n
    }
    des()
  }
}


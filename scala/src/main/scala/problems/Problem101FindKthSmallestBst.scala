
package problems
import scala.collection.mutable

object Problem101FindKthSmallestBst {
  final class Node(var v:Int, var l:Node, var r:Node)
  def kthSmallest(root: Node, k:Int): Int = {
    val st = new mutable.ArrayDeque[Node]()
    var cur: Node = root; var count = 0
    while (cur!=null || st.nonEmpty) {
      while (cur!=null) { st.append(cur); cur = cur.l }
      cur = st.removeLast()
      count += 1
      if (count==k) return cur.v
      cur = cur.r
    }
    -1
  }
}


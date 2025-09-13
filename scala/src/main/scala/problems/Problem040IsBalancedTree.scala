
package problems

object Problem040IsBalancedTree {
  final class Node(var v:Int, var l:Node, var r:Node)
  def isBalanced(n: Node): Boolean = height(n) != -1
  private def height(n: Node): Int = {
    if (n == null) return 0
    val lh = height(n.l); if (lh == -1) return -1
    val rh = height(n.r); if (rh == -1) return -1
    if (math.abs(lh - rh) > 1) -1 else math.max(lh, rh) + 1
  }
}



package problems

object Problem041LowestCommonAncestor {
  final class Node(var v:Int, var l:Node, var r:Node)
  def lca(root: Node, p: Node, q: Node): Node = {
    if (root==null || root eq p || root eq q) return root
    val L = lca(root.l, p, q); val R = lca(root.r, p, q)
    if (L!=null && R!=null) root else if (L!=null) L else R
  }
}


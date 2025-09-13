package problems
object Problem087BinarySearchTreeDelete {
  final class Node(var v:Int, var l:Node, var r:Node)
  def delete(root: Node, key:Int): Node = {
    if (root==null) return null
    if (key < root.v) { root.l = delete(root.l,key); root }
    else if (key > root.v) { root.r = delete(root.r,key); root }
    else {
      if (root.l==null) return root.r
      if (root.r==null) return root.l
      var s = root.r
      while (s.l!=null) s = s.l
      root.v = s.v
      root.r = delete(root.r, s.v)
      root
    }
  }
}

package problems
object Problem086BinarySearchTreeInsert {
  final class Node(var v:Int, var l:Node, var r:Node)
  def insert(root: Node, x:Int): Node = {
    if (root==null) return new Node(x,null,null)
    if (x < root.v) root.l = insert(root.l, x) else root.r = insert(root.r, x)
    root
  }
}

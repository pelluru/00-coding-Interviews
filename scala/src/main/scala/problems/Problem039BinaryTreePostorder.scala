package problems
object Problem039BinaryTreePostorder {
  final class Node(var v:Int, var l:Node, var r:Node)
  def traverse(n:Node): List[Int] = if (n==null) Nil else traverse(n.l) ::: traverse(n.r) ::: List(n.v)
}

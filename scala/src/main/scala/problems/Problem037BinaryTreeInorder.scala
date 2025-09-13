package problems
object Problem037BinaryTreeInorder {
  final class Node(var v:Int, var l:Node, var r:Node)
  def traverse(n:Node): List[Int] = if (n==null) Nil else traverse(n.l) ::: List(n.v) ::: traverse(n.r)
}

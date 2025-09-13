package problems
object Problem038BinaryTreePreorder {
  final class Node(var v:Int, var l:Node, var r:Node)
  def traverse(n:Node): List[Int] = if (n==null) Nil else List(n.v) ::: traverse(n.l) ::: traverse(n.r)
}

package problems
object Problem010DetectCycleLinkedList {
  final class Node(var v:Int, var next:Node)
  object Node { def apply(v:Int)= new Node(v,null) }
  def hasCycle(h:Node): Boolean = {
    var s=h; var f=h
    while (f!=null && f.next!=null) {
      s=s.next; f=f.next.next
      if (s eq f) return true
    }
    false
  }
}

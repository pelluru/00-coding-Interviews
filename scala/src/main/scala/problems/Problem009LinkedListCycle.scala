
package problems

object Problem009LinkedListCycle {
  final class Node(var v:Int, var next: Node)
  object Node { def apply(v:Int): Node = new Node(v, null) }

  def hasCycle(h: Node): Boolean = {
    var slow = h; var fast = h
    while (fast!=null && fast.next!=null) {
      slow = slow.next; fast = fast.next.next
      if (slow eq fast) return true
    }
    false
  }
}


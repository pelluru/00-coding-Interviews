
package problems

object Problem008MergeTwoSortedLists {
  final class ListNode(var x:Int, var next: ListNode)
  object ListNode { def apply(x:Int): ListNode = new ListNode(x, null) }

  def merge(a: ListNode, b: ListNode): ListNode = {
    val dummy = ListNode(0)
    var t = dummy; var p=a; var q=b
    while (p!=null && q!=null) {
      if (p.x < q.x) { t.next = p; p = p.next }
      else { t.next = q; q = q.next }
      t = t.next
    }
    t.next = if (p!=null) p else q
    dummy.next
  }
}


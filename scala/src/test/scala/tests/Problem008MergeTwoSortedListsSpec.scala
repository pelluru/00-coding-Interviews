
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem008MergeTwoSortedLists

class Problem008MergeTwoSortedListsSpec extends AnyFunSuite {
  test("merge two sorted lists") {
    val A = Problem008MergeTwoSortedLists.ListNode(1); A.next = Problem008MergeTwoSortedLists.ListNode(3)
    val B = Problem008MergeTwoSortedLists.ListNode(2); B.next = Problem008MergeTwoSortedLists.ListNode(4)
    val R = Problem008MergeTwoSortedLists.merge(A,B)
    assert(R.x == 1 && R.next.x == 2)
  }
}


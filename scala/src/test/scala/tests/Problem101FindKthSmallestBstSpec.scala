
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem101FindKthSmallestBst

class Problem101FindKthSmallestBstSpec extends AnyFunSuite {
  test("kth smallest BST") {
    val r = new Problem101FindKthSmallestBst.Node(2, new Problem101FindKthSmallestBst.Node(1,null,null), new Problem101FindKthSmallestBst.Node(3,null,null))
    assert(Problem101FindKthSmallestBst.kthSmallest(r,2) === 2)
  }
}


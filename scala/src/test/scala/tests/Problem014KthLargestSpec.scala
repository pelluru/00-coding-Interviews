package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem014KthLargest

class Problem014KthLargestSpec extends AnyFunSuite {
  test("kth largest") {
    assert(Problem014KthLargest.kthLargest(Array(3,2,1,5,6,4),3) === 3)
  }
}

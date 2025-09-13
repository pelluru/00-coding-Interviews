package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem011BinarySearch

class Problem011BinarySearchSpec extends AnyFunSuite {
  test("search") {
    assert(Problem011BinarySearch.search(Array(1,2,3,4), 3) === 2)
  }
}

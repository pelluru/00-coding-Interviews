
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem055MedianTwoSortedArraysSmall

class Problem055MedianTwoSortedArraysSmallSpec extends AnyFunSuite {
  test("median") {
    assert(math.abs(Problem055MedianTwoSortedArraysSmall.median(Array(1,3), Array(2)) - 2.0) < 1e-9)
    assert(math.abs(Problem055MedianTwoSortedArraysSmall.median(Array(1,2), Array(3,4)) - 2.5) < 1e-9)
  }
}


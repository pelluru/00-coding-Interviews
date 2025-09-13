
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem096SlidingWindowMax

class Problem096SlidingWindowMaxSpec extends AnyFunSuite {
  test("sliding window max") {
    assert(Problem096SlidingWindowMax.maxSliding(Array(1,3,-1,-3,5,3,6,7),3).toList == List(3,3,5,5,6,7))
  }
}


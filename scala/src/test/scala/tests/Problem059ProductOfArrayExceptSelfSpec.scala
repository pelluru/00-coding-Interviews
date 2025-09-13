
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem059ProductOfArrayExceptSelf

class Problem059ProductOfArrayExceptSelfSpec extends AnyFunSuite {
  test("product except self") {
    assert(Problem059ProductOfArrayExceptSelf.productExceptSelf(Array(1,2,3,4)).toList == List(24,12,8,6))
  }
}


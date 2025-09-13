package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem015KadaneMaxSubarray

class Problem015KadaneMaxSubarraySpec extends AnyFunSuite {
  test("kadane") {
    assert(Problem015KadaneMaxSubarray.maxSubarray(Array(-2,1,-3,4,-1,2,1,-5,4)) === 6)
  }
}

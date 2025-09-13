package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem027TwoSum

class Problem027TwoSumSpec extends AnyFunSuite {
  test("two sum") {
    assert(Problem027TwoSum.twoSum(Array(2,7,11,15), 9).sameElements(Array(0,1)))
  }
}

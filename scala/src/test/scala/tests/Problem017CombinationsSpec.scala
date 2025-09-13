package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem017Combinations

class Problem017CombinationsSpec extends AnyFunSuite {
  test("combine size") {
    assert(Problem017Combinations.combine(4,2).size === 6)
  }
}

package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem018PowerSet

class Problem018PowerSetSpec extends AnyFunSuite {
  test("power set size") {
    assert(Problem018PowerSet.powerSet(Array(1,2,3)).size === 8)
  }
}

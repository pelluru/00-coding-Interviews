
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem105PowerSetIterative

class Problem105PowerSetIterativeSpec extends AnyFunSuite {
  test("ps iterative size") {
    assert(Problem105PowerSetIterative.powerSet(Array(1,2,3)).size === 8)
  }
}


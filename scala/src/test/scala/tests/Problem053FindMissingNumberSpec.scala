
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem053FindMissingNumber

class Problem053FindMissingNumberSpec extends AnyFunSuite {
  test("missing") {
    assert(Problem053FindMissingNumber.missing(Array(0,1,3)) === 2)
  }
}


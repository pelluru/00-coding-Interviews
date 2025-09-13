
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem102CountSetBits

class Problem102CountSetBitsSpec extends AnyFunSuite {
  test("count bits") {
    assert(Problem102CountSetBits.countBits(0b1011) === 3)
  }
}


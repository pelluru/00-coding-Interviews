
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem052SpiralMatrix

class Problem052SpiralMatrixSpec extends AnyFunSuite {
  test("spiral 3x3") {
    val m = Array(Array(1,2,3),Array(4,5,6),Array(7,8,9))
    assert(Problem052SpiralMatrix.spiral(m) == List(1,2,3,6,9,8,7,4,5))
  }
}



package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem104GrayCode

class Problem104GrayCodeSpec extends AnyFunSuite {
  test("gray code 2") {
    assert(Problem104GrayCode.grayCode(2) == List(0,1,3,2))
  }
}


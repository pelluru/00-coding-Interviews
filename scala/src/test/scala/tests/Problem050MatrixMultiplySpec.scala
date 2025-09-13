
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem050MatrixMultiply

class Problem050MatrixMultiplySpec extends AnyFunSuite {
  test("multiply 2x2") {
    val A = Array(Array(1,2), Array(3,4))
    val B = Array(Array(5,6), Array(7,8))
    val R = Problem050MatrixMultiply.multiply(A,B)
    assert(R(0).toList == List(19,22))
    assert(R(1).toList == List(43,50))
  }
}


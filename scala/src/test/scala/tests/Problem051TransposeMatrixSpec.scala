
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem051TransposeMatrix

class Problem051TransposeMatrixSpec extends AnyFunSuite {
  test("transpose") {
    val M = Array(Array(1,2,3), Array(4,5,6))
    val T = Problem051TransposeMatrix.transpose(M)
    assert(T(0).toList==List(1,4) && T(1).toList==List(2,5) && T(2).toList==List(3,6))
  }
}


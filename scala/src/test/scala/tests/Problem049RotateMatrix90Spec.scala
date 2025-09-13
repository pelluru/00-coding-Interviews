
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem049RotateMatrix90

class Problem049RotateMatrix90Spec extends AnyFunSuite {
  test("rotate 2x2") {
    val m = Array(Array(1,2), Array(3,4))
    Problem049RotateMatrix90.rotate(m)
    assert(m.map(_.toList).toList == List(List(3,1), List(4,2)))
  }
}


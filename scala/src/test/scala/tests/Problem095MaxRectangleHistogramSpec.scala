
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem095MaxRectangleHistogram

class Problem095MaxRectangleHistogramSpec extends AnyFunSuite {
  test("largest rectangle") {
    assert(Problem095MaxRectangleHistogram.largestRectangleArea(Array(2,1,5,6,2,3)) === 10)
  }
}


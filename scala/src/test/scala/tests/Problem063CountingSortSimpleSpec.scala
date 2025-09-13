
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem063CountingSortSimple

class Problem063CountingSortSimpleSpec extends AnyFunSuite {
  test("counting sort") {
    val a = Array(3,1,2,1,0)
    assert(Problem063CountingSortSimple.sort(a,3).toList == List(0,1,1,2,3))
  }
}


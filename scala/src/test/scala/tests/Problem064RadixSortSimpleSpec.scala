
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem064RadixSortSimple

class Problem064RadixSortSimpleSpec extends AnyFunSuite {
  test("radix sort") {
    val a = Array(170,45,75,90,802,24,2,66)
    assert(Problem064RadixSortSimple.sort(a).toList == List(2,24,45,66,75,90,170,802))
  }
}


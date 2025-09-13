
package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem061HeapSort

class Problem061HeapSortSpec extends AnyFunSuite {
  test("heap sort") {
    val a = Array(4,1,3,2); Problem061HeapSort.sort(a)
    assert(a.toList == List(1,2,3,4))
  }
}


package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem013QuickSort

class Problem013QuickSortSpec extends AnyFunSuite {
  test("quicksort") {
    val a = Array(3,1,2); Problem013QuickSort.sort(a); assert(a.sameElements(Array(1,2,3)))
  }
}

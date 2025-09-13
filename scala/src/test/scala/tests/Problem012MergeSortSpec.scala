package tests
import org.scalatest.funsuite.AnyFunSuite
import problems.Problem012MergeSort

class Problem012MergeSortSpec extends AnyFunSuite {
  test("mergesort") {
    assert(Problem012MergeSort.sort(Array(3,1,2)).sameElements(Array(1,2,3)))
  }
}
